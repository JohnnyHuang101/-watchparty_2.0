# --- Stage 1: Build the Go Binary ---
FROM golang:1.26-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy the dependency tracking files first to leverage Docker caching
COPY go.mod go.sum ./
RUN go mod download

# Copy your main.go file into the container
COPY main.go ./
# COPY public/ ./public/


# Compile the application into a statically-linked binary named "watchparty"
# CGO_ENABLED=0 ensures it runs flawlessly inside an empty linux container
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o watchparty main.go

# --- Stage 2: Create the absolute minimal runtime image ---
FROM scratch

# Copy the compiled binary from the builder stage
COPY --from=builder /app/watchparty /watchparty

# Expose port 8080 (or whatever port your main.go listens on) 

EXPOSE 8080

# Run the binary directly
ENTRYPOINT ["/watchparty"]