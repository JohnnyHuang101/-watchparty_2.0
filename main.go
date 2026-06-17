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
	Type         string  `json:"type"` // "identity", "chat", "play", "pause", "seek", "video_change", "user_leave"
	Username     string  `json:"username"`
	Content      string  `json:"content"`       // Custom strings or video urls
	VideoTime    float64 `json:"video_time"`    // Playback sync timestamps
	SourceServer string  `json:"source_server"` // Origin node verification identifier
}

type ActionType int

const (
	ActionConnect ActionType = iota
	ActionDisconnect
	ActionBroadcast
)

type HubAction struct {
	Type ActionType
	Conn *websocket.Conn
	Msg  Message
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

	// Static Assets and Templates Router
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/home.html", http.StatusMovedPermanently)
			return
		}
		fs := http.FileServer(http.Dir("./public"))
		fs.ServeHTTP(w, r)
	})

	// Real-Time Stateful Ingress Pipeline
	http.HandleFunc("/ws", handleConnections)

	// Spin up detached core engine contexts
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

	for action := range hubChannel {
		switch action.Type {
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
			// Hydrate the incoming message payload with the client's generated name if empty
			for conn, client := range localClients {
				outboundMsg := action.Msg

				// If the broadcast packet doesn't have an origin handle assigned yet,
				// fall back to the name tracked inside this node instance's memory context.
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

		// Broadcast configuration payload execution straight to global cluster broker
		msgBytes, _ := json.Marshal(msg)
		nc.Publish(NatsSubject, msgBytes)
	}
}

func handleNatsMessages() {
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
