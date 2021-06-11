package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import com.twitter.chill.KryoPool;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

public class LargeMessageProxy extends AbstractLoggingActor {

    ////////////////////
    // Message Buffer //
    ////////////////////

    // holds information about the large message to be buffered,
    // as well as the message chunks themselves
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class LargeMessageBuffer {
        public int chunkCount;
        public int numberOfChunks;
        public byte[][] messageChunks;

        public void bufferMessagePartFromChunk(int chunkID, byte[] chunkData) {
            this.messageChunks[chunkID] = chunkData;
            this.chunkCount++;
        }
    }

    // used for keeping the different large message buffers based on their ID
    private final HashMap<Integer, LargeMessageBuffer> incomingLargeMessages = new HashMap<>();

    // used to parse meta data ints out of the bytes message
    private static int byteArrayToInt(byte[] byteArray) {
        return ByteBuffer.wrap(byteArray).getInt();
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "largeMessageProxy";

    // determine maximum byte size we are able to send, by reserving 12 bytes for meta information
    final int metaInfoBytes = 12;
    final int chunkBytes = 1024 - metaInfoBytes; // todo: this can still be fine-tuned
    HashMap<Integer,byte[]> outgoingLargeMessages;


    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessage<T> implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private T message;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BytesMessage<T> implements Serializable {
        private static final long serialVersionUID = 4057807743872319842L;
        private T bytes;
        private ActorRef sender;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RequestMessage implements Serializable {
        private static final long serialVersionUID = 7157867778812214852L;
        private int messageID;
        private int lastChunkRead;
        private int chunkCount;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CompletionMessage implements Serializable {
        private static final long serialVersionUID = 7157667278312114859L;
        private int messageID;
    }

    /////////////////
    // Actor State //
    /////////////////

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LargeMessage.class, this::handle)
                .match(BytesMessage.class, this::handle)
                .match(RequestMessage.class, this::handle)
                .match(CompletionMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(LargeMessage<?> largeMessage) {
        Object message = largeMessage.getMessage();
        ActorRef sender = this.sender();
        ActorRef receiver = largeMessage.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

        // serialise message with kryo
        KryoPool kryo = KryoPoolSingleton.get();
        byte[] messageBytes = kryo.toBytesWithClass(message);

        if (messageBytes == null) {
            return;
        }

        // how many chunks do we need to send in order to transmit the message?
        int chunkCount = (int) Math.ceil(messageBytes.length / (double) chunkBytes);

        // determine random message id used for reassembly
        int messageID = (int) Math.floor(Math.random() * Integer.MAX_VALUE);

        this.outgoingLargeMessages.put(messageID,messageBytes);
        byte[] messageIdBytes = ByteBuffer.allocate(4).putInt(messageID).array();
        byte[] chunkCountBytes = ByteBuffer.allocate(4).putInt(chunkCount).array();

        // for each chunk of large message
        int i=0;

        byte[] chunkIDBytes = ByteBuffer.allocate(4).putInt(i).array();

        // split data into chunks
        byte[] dataBytes = Arrays
                .copyOfRange(messageBytes, (0), Math.min(((i + 1) * chunkBytes), messageBytes.length));
        try {
            ByteArrayOutputStream chunkOutput = new ByteArrayOutputStream();

            // write 12 bites used for identifying message, total number of chunks, and which chunk is transmitted
            chunkOutput.write(messageIdBytes);
            chunkOutput.write(chunkCountBytes);
            chunkOutput.write(chunkIDBytes);

            // write actual message chunk
            chunkOutput.write(dataBytes);

            // convert message to byte array
            byte[] messageChunk = chunkOutput.toByteArray();

            // send chunk to receiver
            BytesMessage<byte[]> chunkMessage = new BytesMessage<>(messageChunk, sender, receiver);
            receiverProxy.tell(chunkMessage, this.self());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void handle(BytesMessage<?> message) {
        // collect message data
        byte[] messageChunk = (byte[]) message.getBytes();

        // which large message does this chunk belong to?
        int messageID = byteArrayToInt(Arrays.copyOfRange(messageChunk, 0, 4));

        // how many chunks are there in total?
        int numberOfChunks = byteArrayToInt(Arrays.copyOfRange(messageChunk, 4, 8));

        // which chunk have we received?
        int chunkID = byteArrayToInt(Arrays.copyOfRange(messageChunk, 8, 12));

        // read message data excluding meta info bytes
        byte[] messageData = Arrays.copyOfRange(messageChunk, 12, messageChunk.length);

        // get buffer for the large message id
        LargeMessageBuffer buffer = incomingLargeMessages.get(messageID);

        // init message buffer for new message id if it does not exist, with the expected number of chunks
        if (buffer == null) {
            buffer = new LargeMessageBuffer(0, numberOfChunks, new byte[numberOfChunks][]);
            incomingLargeMessages.put(messageID, buffer);
        }

        // add chunk to buffer
        buffer.bufferMessagePartFromChunk(chunkID, messageData);

        // check whether message is complete and we have buffered all chunks
        if (buffer.getChunkCount() == numberOfChunks && !Arrays.asList(buffer.getMessageChunks()).contains(null)) {

            // remove buffer entry to free the message id for future messages
            incomingLargeMessages.remove(messageID);

            // reassemble the buffered message
            byte[] completeMessage = null;
            try {
                ByteArrayOutputStream messageAssemblyOutput = new ByteArrayOutputStream();
                for (byte[] chunk : buffer.getMessageChunks()) {
                    messageAssemblyOutput.write(chunk);
                }
                completeMessage = messageAssemblyOutput.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
            }

            assert completeMessage != null;

            // deserialize message with Kryo
            KryoPool kryo = KryoPoolSingleton.get();
            Object messageObject = kryo.fromBytes(completeMessage);

            // redirect message to actual target we proxy here
            BytesMessage<Object> messageToReceiver = new BytesMessage<>(messageObject, this.sender(), message.getReceiver());
            message.getReceiver().tell(messageToReceiver.getBytes(), message.getSender());
            message.getSender().tell(new CompletionMessage(messageID), this.self());
        } else {
            this.sender().tell(new RequestMessage(messageID, chunkID, numberOfChunks), this.self());
        }
    }

    private void handle(RequestMessage requestMessage) {
        byte[] messageBytes = outgoingLargeMessages.get(requestMessage.getMessageID());

        int i = requestMessage.getLastChunkRead() +1;
        byte[] chunkIDBytes = ByteBuffer.allocate(4).putInt(i).array();

        byte[] messageIdBytes = ByteBuffer.allocate(4).putInt(requestMessage.getMessageID()).array();
        byte[] chunkCountBytes = ByteBuffer.allocate(4).putInt(requestMessage.getChunkCount()).array();

        // split data into chunks
        byte[] dataBytes = Arrays
                .copyOfRange(messageBytes, (i * chunkBytes), Math.min(((i + 1) * chunkBytes), messageBytes.length));
        try {
            ByteArrayOutputStream chunkOutput = new ByteArrayOutputStream();

            // write 12 bites used for identifying message, total number of chunks, and which chunk is transmitted
            chunkOutput.write(messageIdBytes);
            chunkOutput.write(chunkCountBytes);
            chunkOutput.write(chunkIDBytes);

            // write actual message chunk
            chunkOutput.write(dataBytes);

            // convert message to byte array
            byte[] messageChunk = chunkOutput.toByteArray();

            // send chunk to receiver
            BytesMessage<byte[]> chunkMessage = new BytesMessage<>(messageChunk, this.self(), this.sender());
            this.sender().tell(chunkMessage, this.self());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handle(CompletionMessage completionMessage) {
        int messageID = completionMessage.getMessageID();
        this.outgoingLargeMessages.remove(messageID);
    }
}
