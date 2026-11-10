import argparse
import asyncio
import json
import os
import re
import socket
import sys
import threading
import time

DEFAULT_PORT = 5050
MAX_MESSAGE_LENGTH = 500


def now() -> str:
    return time.strftime("%H:%M")


def encode(message: dict) -> bytes:
    """Every message is one line of JSON."""
    return (json.dumps(message) + "\n").encode("utf-8")


class ChatServer:
    def __init__(self) -> None:
        self.clients: dict[str, asyncio.StreamWriter] = {}

    async def send(self, writer: asyncio.StreamWriter, **message) -> None:
        writer.write(encode(message))
        await writer.drain()

    async def broadcast(self, exclude: str | None = None, **message) -> None:
        for name, writer in list(self.clients.items()):
            if name != exclude:
                try:
                    await self.send(writer, **message)
                except ConnectionError:
                    pass  

    async def login(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> str | None:
        """The first line a client sends must be {"type": "join", "name": ...}."""
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=10)
            name = str(json.loads(line).get("name", "")).strip()
        except (asyncio.TimeoutError, ValueError, AttributeError):
            name = ""

        if not re.fullmatch(r"\w{1,20}", name):
            await self.send(writer, type="error", text="Name must be 1-20 letters, digits or underscores.")
            return None
        if name in self.clients:
            await self.send(writer, type="error", text=f"The name '{name}' is already taken.")
            return None

        self.clients[name] = writer
        await self.send(writer, type="system", text=f"Welcome, {name}! Try /users, /w <name> <message>, /quit.")
        return name

    async def whisper(self, sender: str, writer: asyncio.StreamWriter, text: str) -> None:
        parts = text.split(" ", 2)
        if len(parts) < 3 or not parts[2].strip():
            await self.send(writer, type="system", text="Usage: /w <name> <message>")
            return
        target = self.clients.get(parts[1])
        if target is None:
            await self.send(writer, type="system", text=f"There is no user called {parts[1]}.")
            return
        body = parts[2].strip()
        await self.send(target, type="msg", sender=f"{sender} (private)", text=body, time=now())
        await self.send(writer, type="msg", sender=f"you -> {parts[1]}", text=body, time=now())

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        name = None
        try:
            name = await self.login(reader, writer)
            if name is None:
                return
            await self.broadcast(exclude=name, type="system", text=f"{name} joined the chat.")

            async for raw in reader:
                try:
                    text = str(json.loads(raw).get("text", "")).strip()[:MAX_MESSAGE_LENGTH]
                except (ValueError, AttributeError):
                    continue  
                if not text:
                    continue
                if text == "/users":
                    await self.send(writer, type="system", text="Online: " + ", ".join(sorted(self.clients)))
                elif text.startswith("/w "):
                    await self.whisper(name, writer, text)
                else:
                    await self.broadcast(type="msg", sender=name, text=text, time=now())
        except (ConnectionError, ValueError):
            pass  
        finally:
            if name is not None and self.clients.get(name) is writer:
                del self.clients[name]
                await self.broadcast(type="system", text=f"{name} left the chat.")
            writer.close()


async def serve(host: str, port: int) -> None:
    server = await asyncio.start_server(ChatServer().handle, host, port)
    print(f"Chat server running on {host}:{port}  (Ctrl+C to stop)")
    async with server:
        await server.serve_forever()


def format_message(msg: dict) -> str:
    kind = msg.get("type")
    if kind == "msg":
        return f"[{msg.get('time', '')}] {msg.get('sender', '?')}: {msg.get('text', '')}"
    if kind == "error":
        return f"! {msg.get('text', '')}"
    return f"* {msg.get('text', '')}"


def receive(sock: socket.socket, done: threading.Event) -> None:
    """Runs in a background thread and prints whatever the server sends."""
    with sock.makefile("r", encoding="utf-8") as stream:
        for line in stream:
            try:
                print(format_message(json.loads(line)), flush=True)
            except (ValueError, AttributeError):
                continue
    if not done.is_set():
        print("Disconnected from server. Press Enter to exit.", flush=True)
        done.set()


def run_client(host: str, port: int, name: str) -> None:
    if os.name == "nt":
        os.system("")  
    try:
        sock = socket.create_connection((host, port), timeout=5)
    except OSError as error:
        sys.exit(f"Could not connect to {host}:{port} ({error})")
    sock.settimeout(None)
    sock.sendall(encode({"type": "join", "name": name}))

    done = threading.Event()
    threading.Thread(target=receive, args=(sock, done), daemon=True).start()
    try:
        while not done.is_set():
            text = input()
            if text.strip() == "/quit":
                break
            if text.strip():
                if sys.stdout.isatty():
                    print("\x1b[1A\x1b[2K", end="")  # erase what you typed; the server echoes it back
                sock.sendall(encode({"type": "msg", "text": text}))
    except (EOFError, KeyboardInterrupt, OSError):
        pass
    finally:
        done.set()
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Terminal chat app")
    sub = parser.add_subparsers(dest="role", required=True)
    server_cmd = sub.add_parser("server", help="run the chat server")
    client_cmd = sub.add_parser("client", help="join a chat")
    server_cmd.add_argument("--host", default="127.0.0.1")
    client_cmd.add_argument("--host", default="127.0.0.1")
    client_cmd.add_argument("--name", help="your chat name")
    for command in (server_cmd, client_cmd):
        command.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()

    if args.role == "server":
        try:
            asyncio.run(serve(args.host, args.port))
        except KeyboardInterrupt:
            print("\nServer stopped.")
    else:
        run_client(args.host, args.port, args.name or input("Your name: ").strip())


if __name__ == "__main__":
    main()
