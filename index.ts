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
  errorCloseTimer?: ReturnType<typeof setTimeout>,
  appWindowStartedAt?: number,
  appMessagesInWindow?: number,
  appBytesInWindow?: number,
  invalidMessages?: number;
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
const pointerColorFormat: RegExp = /^[0-9A-F]{8}$/i;
const pointerIdFormat: RegExp = /^[0-9A-F]{32}$/i;
const maxAppEnvelopeBytes = 64 * 1024;
const maxAppMessagesPerSecond = 240;
const maxAppBytesPerSecond = 2 * 1024 * 1024;
const maxTrackedPointerIds = 100_000;
const maxActivePointerStrokes = 10_000;
const maxActivePointerMeasures = 10_000;

type PointerOwnerRecord = { playerId: string; active: boolean };
const pointerStrokeOwners = new Map<string, PointerOwnerRecord>();
const pointerMeasureOwners = new Map<string, PointerOwnerRecord>();
let activePointerStrokes = 0;
let activePointerMeasures = 0;

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
        const rawMessage = message.toString();
        const rawBytes = Buffer.byteLength(rawMessage, "utf8");
        if (rawBytes > maxAppEnvelopeBytes) {
          if (ws.data.joined)
            rejectClientMessage(ws, "MESSAGE_TOO_LARGE", "WebSocket message exceeds 64 KiB UTF-8.");
          else
            rejectJoin(ws, "MESSAGE_TOO_LARGE", "WebSocket message exceeds 64 KiB UTF-8.");
          return;
        }
        const data: any = JSON.parse(rawMessage);
        
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

        if (data?.type === "app") {
          handleAppMessage(ws, data, rawMessage);
          return;
        }

        rejectClientMessage(ws, "UNKNOWN_MESSAGE_TYPE", "Message type is not supported after join.");
      } catch (err) {
        console.error("Erro ao processar mensagem:", err);
        if (!ws.data.joined)
          rejectJoin(ws, "INVALID_MESSAGE", "The first message must be valid JSON.");
        else
          rejectClientMessage(ws, "INVALID_MESSAGE", "Error processing message.");
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
  
  clients.add(ws);
  if (!sendRoomMessage(ws, JSON.stringify(msg_welcome))) {
    ws.data.joined = false;
    clients.delete(ws);
    return;
  }
  
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
    if (client !== ws)
      sendRoomMessage(client, JSON.stringify(msg_joined));
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

  const serialized = JSON.stringify(msg);
  clients.forEach(client => {
    if(client.data.playerId !== ws.data.playerId) sendRoomMessage(client, serialized);
  })
}

var handleAppMessage = (ws: ServerWebSocket<WSData>, data: any, rawMessage: string) => {
  const byteCount = Buffer.byteLength(rawMessage, "utf8");
  if (byteCount > maxAppEnvelopeBytes) {
    rejectClientMessage(ws, "APP_MESSAGE_TOO_LARGE", "Application envelope exceeds 64 KiB UTF-8.");
    return;
  }

  if (data.channel !== "pointer") {
    rejectClientMessage(ws, "UNKNOWN_APP_CHANNEL", "Only the 'pointer' application channel is enabled.");
    return;
  }

  if (typeof data.payload !== "string" || data.payload.trim().length === 0) {
    rejectClientMessage(ws, "INVALID_APP_MESSAGE", "Application payload must be a non-empty string.");
    return;
  }

  if (!consumeAppBudget(ws, byteCount)) {
    sendError(ws, "APP_RATE_LIMITED", "Application message rate limit exceeded.");
    ws.close(1008, "application rate limit");
    return;
  }

  const recipientId = data.to === "" ? null : data.to;
  let recipient: ServerWebSocket<WSData> | undefined;
  if (recipientId != null) {
    if (typeof recipientId !== "string" || recipientId.trim().length === 0 || recipientId.length > 128 || recipientId !== recipientId.trim()) {
      rejectClientMessage(ws, "INVALID_RECIPIENT", "The app recipient must be a valid participant id or null.");
      return;
    }

    if (recipientId === ws.data.playerId) {
      rejectClientMessage(ws, "INVALID_RECIPIENT", "A participant cannot send an app message to itself.");
      return;
    }

    recipient = [...clients].find(client => client.data.playerId === recipientId);
    if (!recipient) {
      rejectClientMessage(ws, "RECIPIENT_NOT_FOUND", "The app recipient is not in this room.");
      return;
    }
  }

  let pointerMessage: any;
  try {
    pointerMessage = JSON.parse(data.payload);
  } catch {
    rejectClientMessage(ws, "INVALID_POINTER_MESSAGE", "Pointer payload must contain valid JSON.");
    return;
  }

  if (!validatePointerMessage(ws, pointerMessage, recipientId))
    return;

  const forwarded = JSON.stringify({
    protocolVersion,
    type: "app",
    playerId: ws.data.playerId,
    channel: "pointer",
    payload: data.payload,
  });
  if (Buffer.byteLength(forwarded, "utf8") > maxAppEnvelopeBytes) {
    rejectClientMessage(ws, "APP_MESSAGE_TOO_LARGE", "Forwarded application envelope exceeds 64 KiB UTF-8.");
    return;
  }

  if (recipient) {
    sendRoomMessage(recipient, forwarded);
    return;
  }

  for (const client of clients) {
    if (client !== ws)
      sendRoomMessage(client, forwarded);
  }
}

var consumeAppBudget = (ws: ServerWebSocket<WSData>, bytes: number): boolean => {
  const now = Date.now();
  const windowStartedAt = ws.data.appWindowStartedAt ?? now;
  if (now - windowStartedAt >= 1000) {
    ws.data.appWindowStartedAt = now;
    ws.data.appMessagesInWindow = 0;
    ws.data.appBytesInWindow = 0;
  } else if (ws.data.appWindowStartedAt == null) {
    ws.data.appWindowStartedAt = now;
  }

  const nextMessages = (ws.data.appMessagesInWindow ?? 0) + 1;
  const nextBytes = (ws.data.appBytesInWindow ?? 0) + bytes;
  if (nextMessages > maxAppMessagesPerSecond || nextBytes > maxAppBytesPerSecond)
    return false;

  ws.data.appMessagesInWindow = nextMessages;
  ws.data.appBytesInWindow = nextBytes;
  return true;
}

var validatePointerMessage = (ws: ServerWebSocket<WSData>, pointer: any, recipient: string | null): boolean => {
  const fail = (code: string, message: string): false => {
    rejectClientMessage(ws, code, message);
    return false;
  };
  const playerId = ws.data.playerId;
  if (!playerId || pointer == null || pointer.v !== 1 || typeof pointer.type !== "string")
    return fail("INVALID_POINTER_MESSAGE", "Pointer message version or type is invalid.");

  const target = pointer.to == null ? null : pointer.to;
  if (target !== recipient)
    return fail("INVALID_RECIPIENT", "Pointer target must match the app envelope recipient.");

  const isFiniteNumber = (value: any): boolean => typeof value === "number" && Number.isFinite(value);
  const validId = (id: any): id is string => typeof id === "string" && pointerIdFormat.test(id);
  const validColor = (color: any): boolean => typeof color === "string" && pointerColorFormat.test(color);
  const ownsActive = (owners: Map<string, PointerOwnerRecord>, id: any): boolean => {
    if (!validId(id)) return false;
    const record = owners.get(id);
    return record?.active === true && record.playerId === playerId;
  };

  switch (pointer.type) {
    case "stroke_begin": {
      if (!validId(pointer.id) || !validColor(pointer.color) || !isFiniteNumber(pointer.width) ||
          pointer.width <= 0 || pointer.width > 0.5)
        return fail("INVALID_POINTER_MESSAGE", "stroke_begin fields are invalid.");
      if (pointerStrokeOwners.has(pointer.id))
        return fail("POINTER_ID_IN_USE", "This stroke id has already been used.");
      if (activePointerStrokes >= maxActivePointerStrokes || pointerStrokeOwners.size >= maxTrackedPointerIds)
        return fail("POINTER_CAPACITY_REACHED", "The room reached its tracked stroke limit.");
      pointerStrokeOwners.set(pointer.id, { playerId, active: true });
      activePointerStrokes++;
      return true;
    }

    case "stroke_points": {
      if (!ownsActive(pointerStrokeOwners, pointer.id) || !Number.isInteger(pointer.start) || pointer.start < 0 ||
          !isFiniteNumber(pointer.q) || pointer.q <= 0 || !Array.isArray(pointer.p) ||
          pointer.p.length === 0 || pointer.p.length % 3 !== 0 || pointer.p.length > 30_000 ||
          !pointer.p.every(Number.isSafeInteger))
        return fail("INVALID_POINTER_MESSAGE", "stroke_points is invalid or not owned by this participant.");
      return true;
    }

    case "stroke_end":
      if (!ownsActive(pointerStrokeOwners, pointer.id))
        return fail("POINTER_NOT_OWNED", "Only the stroke author can finish this stroke.");
      return true;

    case "stroke_remove": {
      if (!ownsActive(pointerStrokeOwners, pointer.id))
        return fail("POINTER_NOT_OWNED", "Only the stroke author can remove this stroke.");
      pointerStrokeOwners.get(pointer.id)!.active = false;
      activePointerStrokes--;
      return true;
    }

    case "measure_add": {
      if (!validId(pointer.id) || !validColor(pointer.color) || !Array.isArray(pointer.ab) ||
          pointer.ab.length !== 6 || !pointer.ab.every(isFiniteNumber))
        return fail("INVALID_POINTER_MESSAGE", "measure_add fields are invalid.");
      if (pointerMeasureOwners.has(pointer.id))
        return fail("POINTER_ID_IN_USE", "This measurement id has already been used.");
      if (activePointerMeasures >= maxActivePointerMeasures || pointerMeasureOwners.size >= maxTrackedPointerIds)
        return fail("POINTER_CAPACITY_REACHED", "The room reached its tracked measurement limit.");
      pointerMeasureOwners.set(pointer.id, { playerId, active: true });
      activePointerMeasures++;
      return true;
    }

    case "measure_remove": {
      if (!ownsActive(pointerMeasureOwners, pointer.id))
        return fail("POINTER_NOT_OWNED", "Only the measurement author can remove this measurement.");
      pointerMeasureOwners.get(pointer.id)!.active = false;
      activePointerMeasures--;
      return true;
    }

    case "strokes_clear":
    case "measures_clear":
      return fail("GLOBAL_CLEAR_FORBIDDEN", "Global clear messages are disabled; remove only objects you authored.");

    case "board_request":
      if (!recipient)
        return fail("INVALID_RECIPIENT", "board_request must target one participant.");
      return true;

    case "board_snapshot":
      if (!recipient || !Number.isInteger(pointer.part) || !Number.isInteger(pointer.parts) ||
          pointer.parts < 1 || pointer.parts > 100_000 || pointer.part < 0 || pointer.part >= pointer.parts ||
          !Array.isArray(pointer.strokes) || (pointer.measures != null && !Array.isArray(pointer.measures)))
        return fail("INVALID_POINTER_MESSAGE", "board_snapshot fields are invalid.");
      return validateAndRegisterSnapshotOwners(ws, pointer, fail);

    case "beam":
      if (typeof pointer.on !== "boolean")
        return fail("INVALID_POINTER_MESSAGE", "beam.on must be a boolean.");
      if (pointer.on && (typeof pointer.hit !== "boolean" || !validColor(pointer.color) ||
          !Array.isArray(pointer.ray) || pointer.ray.length !== 6 || !pointer.ray.every(isFiniteNumber)))
        return fail("INVALID_POINTER_MESSAGE", "beam fields are invalid.");
      return true;

    default:
      return fail("UNKNOWN_POINTER_TYPE", "Pointer message type is not supported.");
  }
}

var validateAndRegisterSnapshotOwners = (
  ws: ServerWebSocket<WSData>,
  pointer: any,
  fail: (code: string, message: string) => false
): boolean => {
  const senderId = ws.data.playerId!;
  const pendingStrokes = new Map<string, PointerOwnerRecord>();
  const pendingMeasures = new Map<string, PointerOwnerRecord>();
  const strokeOwnerFor = (id: string): PointerOwnerRecord | undefined => pendingStrokes.get(id) ?? pointerStrokeOwners.get(id);
  const measureOwnerFor = (id: string): PointerOwnerRecord | undefined => pendingMeasures.get(id) ?? pointerMeasureOwners.get(id);

  for (const stroke of pointer.strokes) {
    if (stroke == null || typeof stroke.id !== "string" || !pointerIdFormat.test(stroke.id) ||
        typeof stroke.author !== "string" || stroke.author.length === 0 ||
        typeof stroke.color !== "string" || !pointerColorFormat.test(stroke.color) || !Number.isFinite(stroke.width) || stroke.width <= 0 ||
        !Number.isInteger(stroke.start) || stroke.start < 0 || typeof stroke.finished !== "boolean" ||
        !Number.isFinite(stroke.q) || stroke.q <= 0 || !Array.isArray(stroke.p) ||
        stroke.p.length % 3 !== 0 || stroke.p.length > 30_000 || !stroke.p.every((value: any) => Number.isSafeInteger(value)))
      return fail("INVALID_POINTER_MESSAGE", "A board_snapshot stroke is invalid.");

    let owner = strokeOwnerFor(stroke.id);
    if (owner == null) {
      if (stroke.author !== senderId)
        return fail("POINTER_AUTHOR_MISMATCH", "A snapshot sender cannot claim another participant's unknown stroke.");
      owner = { playerId: senderId, active: true };
      pendingStrokes.set(stroke.id, owner);
    }
    if (!owner.active || owner.playerId !== stroke.author)
      return fail("POINTER_AUTHOR_MISMATCH", "A snapshot stroke author does not match the server record.");
  }

  if (pointer.measures != null) {
    for (const measure of pointer.measures) {
      if (measure == null || typeof measure.id !== "string" || !pointerIdFormat.test(measure.id) ||
          typeof measure.author !== "string" || measure.author.length === 0 ||
          typeof measure.color !== "string" || !pointerColorFormat.test(measure.color) || !Array.isArray(measure.ab) ||
          measure.ab.length !== 6 || !measure.ab.every(value => typeof value === "number" && Number.isFinite(value)))
        return fail("INVALID_POINTER_MESSAGE", "A board_snapshot measurement is invalid.");

      let owner = measureOwnerFor(measure.id);
      if (owner == null) {
        if (measure.author !== senderId)
          return fail("POINTER_AUTHOR_MISMATCH", "A snapshot sender cannot claim another participant's unknown measurement.");
        owner = { playerId: senderId, active: true };
        pendingMeasures.set(measure.id, owner);
      }
      if (!owner.active || owner.playerId !== measure.author)
        return fail("POINTER_AUTHOR_MISMATCH", "A snapshot measurement author does not match the server record.");
    }
  }

  if (pointerStrokeOwners.size + pendingStrokes.size > maxTrackedPointerIds ||
      pointerMeasureOwners.size + pendingMeasures.size > maxTrackedPointerIds ||
      activePointerStrokes + pendingStrokes.size > maxActivePointerStrokes ||
      activePointerMeasures + pendingMeasures.size > maxActivePointerMeasures)
    return fail("POINTER_CAPACITY_REACHED", "The room reached its tracked marker limit.");

  for (const [id, owner] of pendingStrokes) {
    pointerStrokeOwners.set(id, owner);
    activePointerStrokes++;
  }
  for (const [id, owner] of pendingMeasures) {
    pointerMeasureOwners.set(id, owner);
    activePointerMeasures++;
  }
  return true;
}

var rejectClientMessage = (ws: ServerWebSocket<WSData>, code: string, message: string) => {
  ws.data.invalidMessages = (ws.data.invalidMessages ?? 0) + 1;
  sendError(ws, code, message);
  if ((ws.data.invalidMessages ?? 0) >= 3)
    ws.close(1008, "invalid messages");
}

var sendRoomMessage = (ws: ServerWebSocket<WSData>, message: string): boolean => {
  const result = ws.send(message);
  // Bun returns the number of bytes sent (> 0); -1 means the message was queued
  // successfully but the socket has backpressure. Only 0 means the message dropped.
  if (result > 0)
    return true;

  if (result === -1) {
    console.warn(`Backpressure em ${ws.data.playerId ?? "sem id"}; mensagem enfileirada (${ws.getBufferedAmount()} bytes pendentes).`);
    return true;
  }

  console.error(`Mensagem descartada para ${ws.data.playerId ?? "sem id"}; send retornou ${result}. Fechando a sessão para evitar estado divergente.`);
  ws.close(1013, "outbound message dropped");
  return false;
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

    const serialized = JSON.stringify(msg);
    for (const client of clients) {
      if (client !== ws)
        sendRoomMessage(client, serialized);
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

  sendRoomMessage(ws, JSON.stringify(msg));
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
