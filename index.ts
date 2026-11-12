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
  displayColor?: string,
  sequence?: number,
  closeAfterError?: boolean,
  errorCloseTimer?: ReturnType<typeof setTimeout>;
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
const colorFormat: RegExp = /^[0-9A-F]{6}$/i;

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

    drain(ws){
      if(ws.data.closeAfterError){
        ws.data.closeAfterError = false;
        armErrorCloseTimeout(ws);
      }
    },

    close(ws) {
      clearTimeout(ws.data.errorCloseTimer);

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
        
        if (!validateProtocolVersion(ws, data?.protocolVersion)) return;

        if(!ws.data.joined){
          if (!validateFirstMessage(ws, data?.type)) return;
          
          if (!validateRoomInfo(ws, data?.roomId)) return;

          if (!validateUserInfo(ws, data?.role, data?.displayName, data?.displayColor)) return;

          handleJoin(ws, data);
          return;
        }

        if (data?.type === "pose") {
          if (!validatePose(ws, data?.sequence, data?.root, data?.head, data?.leftHand, data?.rightHand)) return;
          else {
            handlePose(ws, data);
            return;
          }
        }
      } catch (err) {
        console.error("Erro ao processar mensagem:", err);
        if (!ws.data.joined)
          rejectJoin(ws, "INVALID_MESSAGE", "The first message must be valid JSON.");
        else
          sendError(ws, "UNKNOWN_MESSAGE_ERROR", "Error processing message.");
      }
    },
  },
});


var handleJoin = (ws: ServerWebSocket<WSData>, data: any) => {
  const existingPlayers = [...clients].map(client => ({
    playerId: client.data.playerId!,
    role: client.data.role!,
    displayName: client.data.displayName!,
    displayColor: client.data.displayColor!
  }));

  const playerId = randomUUID();
  ws.data.playerId = playerId;
  ws.data.joined = true;
  ws.data.role = data.role;
  ws.data.displayName = data.displayName.trim();
  ws.data.displayColor = data.displayColor.trim();
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
    displayName: ws.data.displayName,
    displayColor: ws.data.displayColor
  };
  
  
  for (const client of clients) {
    client.send(JSON.stringify(msg_joined));
  }
  
  clients.add(ws);
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

var armErrorCloseTimeout = (ws: ServerWebSocket<WSData>) => {
  clearTimeout(ws.data.errorCloseTimer);
  ws.data.errorCloseTimer = setTimeout(() => {
    if(!ws.data.joined) ws.close(1008, "join rejected");
  }, 3000);
}

var rejectJoin = (ws: ServerWebSocket<WSData>, code: string, message: string) => {
  const result = ws.send(JSON.stringify({
    protocolVersion: 1,
    type: "error",
    code,
    message,
  }));

  if (result === 0){
    ws.close(1011, "error send dropped");
    return;
  }

  if (result === -1){
    ws.data.closeAfterError = true;
    return;
  }

  armErrorCloseTimeout(ws);
}

var validateProtocolVersion = (ws: ServerWebSocket<WSData>, _protocolVersion: number) : boolean => {
  if(!_protocolVersion || _protocolVersion !== protocolVersion){
    rejectJoin(ws, "PROTOCOL_VERSION_UNSUPPORTED", `The current protocol version is ${protocolVersion}`);
    return false;
  }
  
  return true;
}

var validateFirstMessage = (ws: ServerWebSocket<WSData>, type: string) : boolean => {
  if (type !== "join") {
    rejectJoin(ws, "INVALID_MESSAGE", "The first message must be join.");
    return false;
  }
  
  return true;
}

var validateRoomInfo = (ws: ServerWebSocket<WSData>, roomId: string): boolean => {
  if(roomId !== roomName){
    rejectJoin(ws, "ROOM_NOT_FOUND", `The room ${roomId} does not exist.`);
    return false;
  }
  
  if(clients.size >= roomCapacity){
    rejectJoin(ws, "ROOM_FULL", `The room reached its capacity of ${roomCapacity} connected users`);
    return false;
  }
  
  return true;
}

var validateUserInfo = (ws: ServerWebSocket<WSData>, role: string, displayName: string, displayColor: string) : boolean => {
  if (role !== "quest_player" && role !== "desktop_viewer") {
    rejectJoin(ws, "INVALID_ROLE", "Role must be either 'quest_player' or 'desktop_viewer'");
    return false;
  }

  if(typeof displayName !== 'string' || displayName.trim().length < 1 || displayName.trim().length > 32){
    rejectJoin(ws, "INVALID_NAME", "Display name must have 1 to 32 characters.");
    return false;
  }

  if(typeof displayColor !== 'string' || !colorFormat.test(displayColor.trim())){
    rejectJoin(ws, "INVALID_COLOR", "Display color must be six hexadecimal RGB characters (RRGGBB), without '#'.");
    return false;
  }

  return true;
}

var validatePose = (ws: ServerWebSocket<WSData>, sequence: any, root: any, head: any, leftHand: any, rightHand: any) : boolean => {
  if(ws.data?.role === "quest_player"){
    if (!validateSequence(ws, sequence)) return false;

    if (!validateTransform(ws, root, "Root", true)) return false;
    if (!validateTransform(ws, head, "Head", true)) return false;
    if (!validateTransform(ws, leftHand, "Left hand", false)) return false;
    if (!validateTransform(ws, rightHand, "Right hand", false)) return false;

    return true;
  }else{
    sendError(ws, "INVALID_POSE", "Pose cannot be sent by an spectator.");
    return false;
  }
}

var validateSequence = (ws: ServerWebSocket<WSData>, sequence: any): boolean =>{
  if(sequence == null){
    sendError(ws, "INVALID_POSE", "Pose sequence is mandatory.");
    return false;
  }

  if (!Number.isInteger(sequence) || sequence < 1 || (ws.data?.sequence && sequence <= ws.data?.sequence) ) {
    sendError(ws, "INVALID_POSE", "Pose sequence must be a positive integer and greater than the last.");
    return false;
  }

  return true;
}

var validateTransform = (ws: ServerWebSocket<WSData>, transform: any, name: string, isTransformMandatory: boolean): boolean =>{
  if(transform == null){ 
    if(isTransformMandatory){
      sendError(ws, "INVALID_POSE", `${name} is mandatory.`);
      return false;
    }
    else
      return true;
  }
  
  const position = transform.position;
  const rotation = transform.rotation;
  const isFiniteNumber = (value: any): boolean =>
    typeof value === 'number' && Number.isFinite(value);

  var position_error: boolean = position == null ||
                                !isFiniteNumber(position.x) ||
                                !isFiniteNumber(position.y) ||
                                !isFiniteNumber(position.z);
  var rotation_error: boolean = rotation == null ||
                                !isFiniteNumber(rotation.x) ||
                                !isFiniteNumber(rotation.y) ||
                                !isFiniteNumber(rotation.z) ||
                                !isFiniteNumber(rotation.w);

  if (!rotation_error) {
    const magnitudeSquared = rotation.x * rotation.x + rotation.y * rotation.y +
                             rotation.z * rotation.z + rotation.w * rotation.w;
    rotation_error = !Number.isFinite(magnitudeSquared) || magnitudeSquared < 1e-12;
  }
  
  if(position_error || rotation_error){
    if(position_error)
      sendError(ws, "INVALID_POSE", `${name} position is mandatory and its coordinates must be numbers.`);
    
    if(rotation_error)
      sendError(ws, "INVALID_POSE", `${name} rotation is mandatory and its coordinates must be numbers.`);
    
    return false;
  }

  return true;
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
