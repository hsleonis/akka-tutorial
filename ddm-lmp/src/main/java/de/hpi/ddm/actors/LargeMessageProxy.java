package de.hpi.ddm.actors;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import java.io.*;
import akka.actor.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";

	private static final int MESSAGE_SIZE = 128;
	private List<byte[]> messages = new LinkedList<>();

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private ActorRef sender;
		private ActorRef receiver;
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
				.match(byte[].class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		//serialize message
		Kryo kryo = new Kryo();
		FieldSerializer fieldSerializer = new FieldSerializer(kryo, LargeMessage.class);
		// receiver attribute won't be serialized, serialize message only
		fieldSerializer.removeField("receiver");
		kryo.register(LargeMessage.class, fieldSerializer);

		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		Output output = new Output(stream);
		kryo.writeObject(output, message);
		output.close();

		byte[] buffer = stream.toByteArray();
		try {
			stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//split buffer into smaller pieces
		List<byte[]> pieces = new LinkedList<>();
		for(int byteIndex = 0; byteIndex < buffer.length; byteIndex += MESSAGE_SIZE) {
			int endOfCopy = Math.min(byteIndex + MESSAGE_SIZE, buffer.length);
			pieces.add(Arrays.copyOfRange(buffer, byteIndex, endOfCopy));
		}

		//send pieces
		for(byte[] piece : pieces) {
			receiverProxy.tell(piece, this.self());
		}

		//end of message, all pieces have been send
		receiverProxy.tell(new BytesMessage<>(this.sender(), message.getReceiver()), this.self());
	}

	private void handle(byte[] message) {
		this.messages.add(message);
	}

	private void handle(BytesMessage<?> message) {
		//join all messages to get complete message
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		for(byte[] i: this.messages) {
			try {
				stream.write(i);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		byte[] buffer = stream.toByteArray();
		try {
			stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		//deserialize message
		Kryo kryo = new Kryo();
		FieldSerializer fieldSerializer = new FieldSerializer(kryo, LargeMessage.class);
		//deserialize message, receiver attribute is not serialized
		fieldSerializer.removeField("receiver");
		kryo.register(LargeMessage.class, fieldSerializer);

		Input input = new Input(new ByteArrayInputStream(buffer));
		LargeMessage deserializedMessage = kryo.readObject(input, LargeMessage.class);
		input.close();

		message.getReceiver().tell(deserializedMessage.getMessage(), message.getSender());
	}
}
