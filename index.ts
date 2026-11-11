/// <reference types="bun-types" />

import type { ServerWebSocket } from "bun";
import { randomUUID } from "node:crypto";
import * as dgram from "node:dgram";

// Agora o ws tem um "data" tipado com playerId
type WSData = { 
  playerId?: string,
  joined?: boolean,
  role?: string,
  displayName?: string,
  sequence?: number,
};

type Vector3 ={
  x: number,
  y: number,
  z: number,
}

type Vector4 ={
  x: number,
  y: number,
  z: number,
  w: number,
}

type transform = {
  position: Vector3,
  rotation: Vector4,
}

// clientes conectados
const clients = new Set<ServerWebSocket<WSData>>();

// clientes tentando conectar
const pendingConnections = new Set<ServerWebSocket<WSData>>();

const roomName: string = "sala-local";
const roomCapacity: number = 5;
const protocolVersion: number = 1;

Bun.serve<WSData>({
  port: 7777,

  fetch(req, server) {
    if (server.upgrade(req, { data: {} })) return;
    return new Response("Servidor WebSocket VR rodando na porta 7777");
  },

  websocket: {
    open(ws) {
      pendingConnections.add(ws);
    },

    close(ws) {
      console.log("Cliente desconectado:", ws.data.playerId ?? "(sem registro)");
      pendingConnections.delete(ws);
      
      if(ws.data.joined){
        clients.delete(ws);
        console.log(`Saiu: ${ws.data.playerId}; participantes existentes: ${clients.size}`);
        notifyRoomPlayerLeft(ws);
      }
    },

    message(ws, message) {
      try {
        const data: any = JSON.parse(message.toString());
        if(!data.protocolVersion ||  data.protocolVersion !== protocolVersion){
          sendError(ws, "PROTOCOL_VERSION_UNSUPPORTED", `The current protocol version is ${protocolVersion}`);
          ws.close();
          return;
        }

        if(!ws.data.joined){
          if (data?.type !== "join") {
            sendError(ws, "INVALID_MESSAGE", "The first message must be join.");
            ws.close();
            return;
          }

          if(data?.roomId !== roomName){
            sendError(ws, "ROOM_NOT_FOUND", `The room ${data?.roomId} does not exist.`);
            ws.close();
            return;
          }

          if(clients.size >= roomCapacity){
            sendError(ws, "ROOM_FULL", `The room reached its capacity of ${roomCapacity} connected users`);
            ws.close();
            return;
          }

          if (data?.role !== "quest_player" && data?.role !== "desktop_viewer") {
            sendError(ws, "INVALID_ROLE", "Role must be either 'quest_player' or 'desktop_viewer'");
            ws.close();
            return;
          }

          if(typeof data.displayName !== "string" || data.displayName.trim().length < 1 || data.displayName.trim().length > 32){
            sendError(ws, "INVALID_MESSAGE", "Display name must have 1 to 32 characters.");
            ws.close();
            return;
          }

          handleJoin(ws, data);
          return;
        }

        if (data?.type === "pose") {
          if(ws.data?.role === "quest_player"){
            if (!Number.isInteger(data.sequence) || data.sequence < 1 || (ws.data?.sequence && data.sequence <= ws.data?.sequence) ) {
              sendError(ws, "INVALID_POSE", "Pose sequence must be a positive integer and greater than the last.");
              return;
            }
            
            if(!data.root || !data.root.position || !data.root.rotation){
              sendError(ws, "INVALID_POSE", "Root/body position is mandatory.");
              return;
            }

            if(!data.head || !data.head.position || !data.head.rotation){
              sendError(ws, "INVALID_POSE", "Head position is mandatory.");
              return;
            }

            handlePose(ws, data);
            return;
          }else{
            sendError(ws, "INVALID_POSE", "Pose cannot be sent by an spectator.");
            return;
          }
        }
      } catch (err) {
        console.error("Erro ao processar mensagem:", err);
        sendError(ws, "UNKNOWN_MESSAGE_ERROR", "Error processing message.")
      }
    },
  },
});


var handleJoin = (ws: ServerWebSocket<WSData>, data: any) => {
  const existingPlayers = [...clients].map(client => ({
    playerId: client.data.playerId!,
    role: client.data.role!,
    displayName: client.data.displayName!,
  }));

  const playerId = randomUUID();
  ws.data.playerId = playerId;
  ws.data.joined = true;
  ws.data.role = data.role;
  ws.data.displayName = data.displayName.trim();
  ws.data.sequence = -1;

  pendingConnections.delete(ws);
  
  // Informar ao cliente que ele foi conectado com sucesso.
  const msg_welcome = {
    protocolVersion: 1,
    type: "welcome",
    sessionId: "sala-local",
    selfId: ws.data.playerId,
    role: data.role,
    players: existingPlayers
  };
  
  ws.send(JSON.stringify(msg_welcome));
  
  // Agora avisa todo mundo (inclusive Unity) que esse player existe.
  const msg_joined = {
    protocolVersion: 1,
    type: "player_joined",
    playerId: ws.data.playerId,
    role: ws.data.role,
    displayName: ws.data.displayName
  };
  
  clients.add(ws);
  
  for (const client of clients) {
    client.send(JSON.stringify(msg_joined));
  }
  
  console.log(`Entrou: ${ws.data.playerId}; participantes existentes: ${clients.size}`);
}

var handlePose = (ws: ServerWebSocket<WSData>, data: any) => {
  ws.data.sequence = data.sequence;

  const msg = {
    protocolVersion: 1,
    type: "player_pose",
    sequence: data.sequence,
    playerId: ws.data.playerId,
    root: {
      position: data.root.position,
      rotation: data.root.rotation,
    },
    head: {
      position: data.head.position,
      rotation: data.head.rotation,
    },
    leftHand: data.leftHand == null ? null : {
      position: data.leftHand.position,
      rotation: data.leftHand.rotation,
    },
    rightHand: data.rightHand == null ? null : {
      position: data.rightHand.position,
      rotation: data.rightHand.rotation,
    },
  };

  clients.forEach(client => {
    if(client.data.playerId !== ws.data.playerId) client.send(JSON.stringify(msg));
  })
}

var notifyRoomPlayerLeft = (ws: ServerWebSocket<WSData>) => {
  // avisa todo mundo quem saiu (se souber o id)
  if (ws.data.playerId) {
    const msg = {
      protocolVersion: 1,
      type: "player_left",
      playerId: ws.data.playerId,
      reason: "disconnected"
    };

    for (const client of clients) {
      client.send(JSON.stringify(msg));
    }
  }
}

var sendError = (ws: ServerWebSocket<WSData>, code: string, message: string) => {
  const msg = {
    protocolVersion: 1,
    type: "error",
    code,
    message
  };

  ws.send(JSON.stringify(msg));
}
// ===== DESCOBERTA NA LAN (beacon UDP) =====
const DISCOVERY_PORT = 47777;               // porta só da descoberta (diferente da 7777)
const beacon = dgram.createSocket({ type: "udp4", reuseAddr: true });

beacon.bind(() => {
  beacon.setBroadcast(true);                // habilita mandar p/ 255.255.255.255
});

setInterval(() => {
  const payload = JSON.stringify({
    service: "djembefola",                  // "assinatura" pro Unity filtrar
    name: "Sala do Djembe",                 // nome que aparece na lista
    wsPort: 7777,                           // porta do seu WebSocket
    players: clients.size,                  // reaproveita seu Set de clientes
  });
  const buf = Buffer.from(payload);
  // broadcast pra todo mundo na rede local, na porta de descoberta
  beacon.send(buf, 0, buf.length, DISCOVERY_PORT, "255.255.255.255");
}, 1000);

console.log("📡 Beacon de descoberta em UDP:${DISCOVERY_PORT}");

console.log("🚀 WebSocket VR rodando em ws://localhost:7777");
