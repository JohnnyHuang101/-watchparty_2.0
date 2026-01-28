package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// 1. Configure the Upgrader
// This validates the handshake. CheckOrigin is crucial for CORS (local dev).
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all connections (change for production!)
	},
}

// 2. Define a "Thread-Safe" Store for Users
var (
	clients   = make(map[*websocket.Conn]bool) // Key: Connection, Value: Connected Status
	broadcast = make(chan Message)             // Broadcast channel
	mutex     = sync.Mutex{}                   // Protects the clients map
)

// Define what a message looks like
type Message struct {
	Type      string  `json:"type"` // "chat", "play", "pause", "seek"
	Username  string  `json:"username"`
	Content   string  `json:"content"`    // Chat text
	VideoTime float64 `json:"video_time"` // Current time in seconds (for seeking/sync)
}

func main() {
	// 1. Serve the Frontend
	// This tells Go to look for an "index.html" inside the "./public" folder
	fs := http.FileServer(http.Dir("./public"))
	http.Handle("/", fs)

	// 2. WebSocket Endpoint
	http.HandleFunc("/ws", handleConnections)

	// 3. Start the Broadcast Loop
	// This runs in the background to send messages to all users
	go handleMessages()

	fmt.Println("Server started on :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

// Handle the Connection (User joins)
func handleConnections(w http.ResponseWriter, r *http.Request) {
	// Upgrade initial GET request to a websocket
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Register the new client
	mutex.Lock()
	clients[ws] = true
	mutex.Unlock()
	fmt.Println("New Client Connected")

	// Cleanup when function returns (user disconnects)
	defer func() {
		mutex.Lock()
		delete(clients, ws)
		mutex.Unlock()
		ws.Close()
		fmt.Println("Client Disconnected")
	}()

	// Infinite loop that reads from *this specific* client
	for {
		var msg Message
		// Read JSON message from client
		err := ws.ReadJSON(&msg)
		if err != nil {
			// Error usually means client disconnected
			break
		}
		// Send the message to the broadcast channel
		broadcast <- msg
	}
}

// Broadcast Loop (The "Fan-Out")
// This sends a single message to every connected user
func handleMessages() {
	for {
		// Grab the next message from the broadcast channel
		msg := <-broadcast

		// Send it to every connected client
		mutex.Lock()
		for client := range clients {
			err := client.WriteJSON(msg)
			if err != nil {
				log.Printf("error: %v", err)
				client.Close()
				delete(clients, client)
			}
		}
		mutex.Unlock()
	}
}
