package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"

	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"go.opentelemetry.io/otel/trace"
)

func initTracer(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint("jaeger:4317"),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceName("watchparty"),
			attribute.String("service.instance.id", serverName),
			attribute.String("deployment.environment", "dev"),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)

	otel.SetTextMapPropagator(
		b3.New(
			b3.WithInjectEncoding(
				b3.B3MultipleHeader | b3.B3SingleHeader,
			),
		),
	)

	return tp, nil
}

type Client struct {
	Name string
}

type Message struct {
	Type         string  `json:"type"`
	Username     string  `json:"username"`
	Content      string  `json:"content"`
	VideoTime    float64 `json:"video_time"`
	SourceServer string  `json:"source_server"`
}

type ActionType int

const (
	ActionConnect ActionType = iota
	ActionDisconnect
	ActionBroadcast
	ActionGetState
)

type RoomState struct {
	LastAction    string      `json:"last_action"`
	LastUpdatedBy string      `json:"last_updated_by"`
	Detail        interface{} `json:"detail"`
	UpdatedAt     time.Time   `json:"updated_at"`
}

type ConnectResult struct {
	ClientName string
	Err        error
}

type HubAction struct {
	Type     ActionType
	Conn     *websocket.Conn
	Msg      Message
	RespChan chan []RoomState

	// Used for tracing/acknowledging connection registration.
	Ctx         context.Context
	ConnectResp chan ConnectResult
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var (
	hubChannel  = make(chan HubAction, 1024)
	nc          *nats.Conn
	serverName  string
	roomHistory []RoomState
)

const NatsSubject = "watchparty.global"

var adjectives = []string{"Bouncy", "Sneaky", "Cosmic", "Wiggly", "Fuzzy", "Chaotic", "Sleepy", "Spicy", "Glorious", "Turbo"}
var animals = []string{"Panda", "Otter", "Raccoon", "Axolotl", "Ferret", "Penguin", "Sloth", "Capybara", "Platypus", "Lemur"}

func randomName() string {
	return fmt.Sprintf("%s%s-%04d", adjectives[rand.Intn(len(adjectives))], animals[rand.Intn(len(animals))], rand.Intn(10000))
}

func main() {

	rand.Seed(time.Now().UnixNano())

	serverName = os.Getenv("SERVER_NAME")
	if serverName == "" {
		serverName = "LOCAL_NODE"
	}

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	ctx := context.Background()

	tp, tracer_err := initTracer(ctx)
	if tracer_err != nil {
		log.Fatalf("failed to initialize tracing: %v", tracer_err)
	}

	defer func() {
		if tracer_err := tp.Shutdown(context.Background()); tracer_err != nil {
			log.Printf("failed to shutdown tracer provider: %v", tracer_err)
		}
	}()

	var err error
	nc, err = nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()

	fmt.Printf("[%s] Connected to NATS at %s\n", serverName, natsURL)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/home.html", http.StatusMovedPermanently)
			return
		}
		fs := http.FileServer(http.Dir("./public"))
		fs.ServeHTTP(w, r)
	})

	http.HandleFunc("/ws", handleConnections)

	// Debug/inspection endpoint — safely reads hub-confined state via channel round-trip
	http.HandleFunc("/debug/state", func(w http.ResponseWriter, r *http.Request) {
		resp := make(chan []RoomState)
		hubChannel <- HubAction{Type: ActionGetState, RespChan: resp}
		state := <-resp
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(state)
	})

	go currentHubManager()
	go handleNatsMessages()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	fmt.Printf("[%s] Cluster node listening actively on :%s\n", serverName, port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// Single-Threaded State Engine (Confinement Actor Pattern)
func currentHubManager() {
	localClients := make(map[*websocket.Conn]*Client)

	// Confined state — ONLY this goroutine ever reads or writes this.
	var roomState RoomState

	for action := range hubChannel {
		switch action.Type {

		case ActionGetState:
			action.RespChan <- roomHistory // struct copy — safe to hand out

		case ActionConnect:
			ctx := action.Ctx
			if ctx == nil {
				ctx = context.Background()
			}

			tracer := otel.Tracer("watchparty")

			ctx, span := tracer.Start(
				ctx,
				"hub.register",
				trace.WithAttributes(
					attribute.String("server.name", serverName),
				),
			)

			clientName := randomName()

			span.SetAttributes(
				attribute.String("client.name", clientName),
			)

			// Actual registration happens here.
			localClients[action.Conn] = &Client{
				Name: clientName,
			}

			span.AddEvent("client registered")

			fmt.Printf(
				"[%s] Hub Engine: Registered local address for user context %s\n",
				serverName,
				clientName,
			)

			// Trace sending the identity message separately.
			_, identitySpan := tracer.Start(ctx, "websocket.identity.write")

			err := action.Conn.WriteJSON(Message{
				Type:     "identity",
				Username: clientName,
			})

			if err != nil {
				identitySpan.RecordError(err)
				identitySpan.SetStatus(
					codes.Error,
					"failed to send websocket identity",
				)
				identitySpan.End()

				span.RecordError(err)
				span.SetStatus(
					codes.Error,
					"client registered but identity write failed",
				)

				// Undo registration because connection setup failed.
				delete(localClients, action.Conn)
				action.Conn.Close()

				span.End()

				if action.ConnectResp != nil {
					action.ConnectResp <- ConnectResult{
						Err: err,
					}
				}

				break
			}

			identitySpan.End()

			span.AddEvent("identity sent")
			span.End()

			if action.ConnectResp != nil {
				action.ConnectResp <- ConnectResult{
					ClientName: clientName,
				}
			}

		case ActionDisconnect:
			if client, exists := localClients[action.Conn]; exists {
				fmt.Printf("[%s] Hub Engine: Revoked local routing tracking for user context %s\n", serverName, client.Name)

				presenceLeaveMsg := Message{
					Type:         "user_leave",
					Username:     "System",
					Content:      fmt.Sprintf("%s has left the room.", client.Name),
					SourceServer: serverName,
				}

				delete(localClients, action.Conn)
				action.Conn.Close()

				msgBytes, err := json.Marshal(presenceLeaveMsg)
				if err == nil {
					nc.Publish(NatsSubject, msgBytes)
				}
			}

		case ActionBroadcast:
			// Update confined room state — safe because only this goroutine touches it,
			// and this goroutine sees messages in the same order NATS delivered them.
			roomState.LastAction = action.Msg.Type
			roomState.LastUpdatedBy = action.Msg.Username
			roomState.UpdatedAt = time.Now()

			// Only stash the fields relevant to this message type, so a
			// "chat" message can't stomp the last-known video position
			// (and vice versa) the way a single shared field used to.
			switch action.Msg.Type {
			case "chat":
				roomState.Detail = map[string]string{
					"message": action.Msg.Content,
				}
			case "seek":
				roomState.Detail = map[string]float64{
					"video_time": action.Msg.VideoTime,
				}
			case "user_join", "user_leave":
				roomState.Detail = map[string]string{
					"content": action.Msg.Content,
				}
			default:
				// Fallback: keep the raw message so nothing silently drops.
				roomState.Detail = action.Msg
			}

			roomHistory = append(roomHistory, roomState)

			for conn, client := range localClients {
				outboundMsg := action.Msg

				if outboundMsg.Username == "" && action.Msg.Type == "chat" {
					outboundMsg.Username = client.Name
				}

				err := conn.WriteJSON(outboundMsg)
				if err != nil {
					conn.Close()
					delete(localClients, conn)
				}
			}
		}
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	tracer := otel.Tracer("watchparty")

	// Recover incoming distributed trace context.
	propagator := otel.GetTextMapPropagator()

	ctx := propagator.Extract(
		r.Context(),
		propagation.HeaderCarrier(r.Header),
	)

	// Parent span for the entire initial WebSocket connection lifecycle.
	ctx, span := tracer.Start(
		ctx,
		"websocket.connect",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("url.path", r.URL.Path),
			attribute.String("server.name", serverName),
		),
	)

	// sc := span.SpanContext()

	// log.Printf(
	// 	"manual span: trace=%s span=%s valid=%v sampled=%v recording=%v",
	// 	sc.TraceID(),
	// 	sc.SpanID(),
	// 	sc.IsValid(),
	// 	sc.IsSampled(),
	// 	span.IsRecording(),
	// )

	// Guarantees span closure on every exit path.
	// defer span.End()

	/*
		STEP 1:
		HTTP -> WebSocket protocol upgrade
	*/
	_, upgradeSpan := tracer.Start(
		ctx,
		"websocket.upgrade",
	)

	ws, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		upgradeSpan.RecordError(err)
		upgradeSpan.SetStatus(
			codes.Error,
			"websocket upgrade failed",
		)
		upgradeSpan.End()

		span.RecordError(err)
		span.SetStatus(
			codes.Error,
			"websocket connection failed during upgrade",
		)

		return
	}

	upgradeSpan.AddEvent("websocket upgraded")
	upgradeSpan.End()

	span.AddEvent("websocket upgraded")

	/*
		From this point forward, make sure the connection
		is eventually removed from the hub.
	*/
	defer func() {
		hubChannel <- HubAction{
			Type: ActionDisconnect,
			Conn: ws,
		}
	}()

	/*
		STEP 2:
		Send registration request to actor/hub.

		We pass ctx so hub.register becomes a child of
		websocket.connect.
	*/
	connectResp := make(chan ConnectResult, 1)

	hubChannel <- HubAction{
		Type:        ActionConnect,
		Conn:        ws,
		Ctx:         ctx,
		ConnectResp: connectResp,
	}

	span.AddEvent("registration queued")

	/*
		STEP 3:
		Wait until the hub actually registers the user.

		This is the important difference from your old version.
	*/
	select {

	case result := <-connectResp:
		if result.Err != nil {
			span.RecordError(result.Err)
			span.SetStatus(
				codes.Error,
				"hub registration failed",
			)
			ws.Close()
			return
		}

		span.SetAttributes(
			attribute.String("client.name", result.ClientName),
		)

		span.AddEvent("connection fully registered")

	case <-time.After(5 * time.Second):
		err := fmt.Errorf("hub registration timed out")

		span.RecordError(err)

		span.SetStatus(
			codes.Error,
			"hub registration timed out",
		)

		ws.Close()
		return

	case <-r.Context().Done():
		err := r.Context().Err()

		span.RecordError(err)

		span.SetStatus(
			codes.Error,
			"request cancelled during registration",
		)

		ws.Close()
		return
	}

	/*
		INITIAL CONNECTION TRACE ENDS HERE.

		defer span.End() will execute when this function returns,
		though, so explicitly ending here would be wrong because
		the handler continues for the lifetime of the socket.

		Instead, if we want websocket.connect to represent ONLY
		initial connection setup, end it now.
	*/

	span.End()

	/*
		The WebSocket is now established.

		Future message traces should be separate spans/traces
		rather than keeping websocket.connect open for potentially
		hours.
	*/

	for {
		var msg Message

		err := ws.ReadJSON(&msg)
		if err != nil {
			break
		}

		msg.SourceServer = serverName

		msgBytes, err := json.Marshal(msg)
		if err != nil {
			continue
		}

		if err := nc.Publish(NatsSubject, msgBytes); err != nil {
			log.Printf(
				"[%s] failed to publish websocket message: %v",
				serverName,
				err,
			)
		}
	}
}

func handleNatsMessages() {
	// Thin, stateless relay — no shared state touched here.
	_, err := nc.Subscribe(NatsSubject, func(m *nats.Msg) {
		var msg Message
		if err := json.Unmarshal(m.Data, &msg); err != nil {
			return
		}

		hubChannel <- HubAction{
			Type: ActionBroadcast,
			Msg:  msg,
		}
	})
	if err != nil {
		log.Fatalf("Fatal: NATS Cluster pipeline connection collapsed: %v", err)
	}
	select {}
}
