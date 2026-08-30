package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
)

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
	LastVideoTime float64   `json:"last_video_time"`
	LastAction    string    `json:"last_action"`
	LastUpdatedBy string    `json:"last_updated_by"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type HubAction struct {
	Type     ActionType
	Conn     *websocket.Conn
	Msg      Message
	RespChan chan RoomState // only populated for ActionGetState
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var (
	hubChannel = make(chan HubAction, 1024)
	nc         *nats.Conn
	serverName string
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
		resp := make(chan RoomState)
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
			action.RespChan <- roomState // struct copy — safe to hand out

		case ActionConnect:
			clientName := randomName()
			localClients[action.Conn] = &Client{Name: clientName}
			fmt.Printf("[%s] Hub Engine: Registered local address for user context %s\n", serverName, clientName)

			action.Conn.WriteJSON(Message{
				Type:     "identity",
				Username: clientName,
			})

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
			roomState.LastVideoTime = action.Msg.VideoTime
			roomState.LastAction = action.Msg.Type
			roomState.LastUpdatedBy = action.Msg.Username
			roomState.UpdatedAt = time.Now()

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
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	hubChannel <- HubAction{Type: ActionConnect, Conn: ws}
	defer func() {
		hubChannel <- HubAction{Type: ActionDisconnect, Conn: ws}
	}()

	for {
		var msg Message
		err := ws.ReadJSON(&msg)
		if err != nil {
			break
		}

		msg.SourceServer = serverName

		msgBytes, _ := json.Marshal(msg)
		nc.Publish(NatsSubject, msgBytes)
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
