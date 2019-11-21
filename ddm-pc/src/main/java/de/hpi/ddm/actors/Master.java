package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	private final Queue<Worker.SeqMessage> unassignedWork = new LinkedList<>();
	private final Queue<Worker.OldMessage> unassignedPasswordWork = new LinkedList<>();
	private final Queue<ActorRef> idleWorkers = new LinkedList<>();
	private final Map<ActorRef, Worker.SeqMessage> busyWorkers = new HashMap<>();
	private final Map<ActorRef, Worker.OldMessage> busyPasswordWorkers = new HashMap<>();

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordMessage implements Serializable {
		private static final long serialVersionUID = -102767440935270949L;
		private String result;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintsCompletedMessage implements Serializable {
		private static final long serialVersionUID = 1670208454683295451L;
		private char missingChar;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordCharMessage implements Serializable {
		private static final long serialVersionUID = 3854781765800714665L;
		private int passwordIndex;
		private char missingChar;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;

	private long startTime;
	private int passwordLength;
	private LinkedList<String> passwordChars = new LinkedList<String>();
	private LinkedList<String> passwords = new LinkedList<String>();

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.match(PasswordMessage.class, this::handle)
				.match(HintsCompletedMessage.class, this::handle)
				.match(PasswordCharMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void handle(PasswordMessage message) {
		this.collector.tell(new Collector.CollectMessage(message.getResult()), this.self());

		this.busyPasswordWorkers.remove(this.sender());

		if (this.unassignedPasswordWork.size() != 0) {
			this.sender().tell(unassignedPasswordWork.element(), this.self());
			this.busyPasswordWorkers.put(this.sender(), unassignedPasswordWork.remove());
		}
		else  {
			this.idleWorkers.add(this.sender());
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
		}
	}

	protected void handle(HintsCompletedMessage message) {
		this.busyWorkers.remove(this.sender());

		if (this.unassignedWork.size() != 0) {
			this.sender().tell(unassignedWork.element(), this.self());
			this.busyWorkers.put(this.sender(), unassignedWork.remove());
		}
		else  {
			if(!this.unassignedPasswordWork.isEmpty()) {
				this.sender().tell(unassignedPasswordWork.element(), this.self());
				this.busyPasswordWorkers.put(this.sender(), unassignedPasswordWork.remove());
			}
			for (int i = 0; i < passwordChars.size(); i++) {
				Worker.OldMessage msg = new Worker.OldMessage();
				msg.setPasswordLength(this.passwordLength);
				msg.setPasswordChars(this.passwordChars.get(i));
				msg.setPassword(this.passwords.get(i));
				this.unassignedPasswordWork.add(msg);
			}
			this.sender().tell(unassignedPasswordWork.element(), this.self());
			this.busyPasswordWorkers.put(this.sender(), unassignedPasswordWork.remove());
		}
	}

	protected void handle (PasswordCharMessage message) {
		// remove 1 of index because IDs start at 1 instead of 0
		int index = message.getPasswordIndex() - 1;
		this.passwordChars.set(index, this.passwordChars.get(index).replace(String.valueOf(message.getMissingChar()), ""));
	}

	protected void handle(BatchMessage message) throws InterruptedException {

		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			return;
		}

		this.passwordLength = Integer.parseInt(message.lines.get(0)[3]);
		HashMap<String, LinkedList<Integer>> hintMap = new HashMap<>();

		for (String[] line : message.getLines()) {

			this.passwordChars.add(line[2]);
			this.passwords.add(line[4]);

			String[] hints = Arrays.copyOfRange(line, 5, line.length);

			for (String hint : hints) {
				if (hintMap.containsKey(hint)) {
					LinkedList<Integer> values = hintMap.get(hint);
					values.add(Integer.parseInt(line[0]));
					hintMap.put(hint, values);
				} else {
					LinkedList<Integer> values = new LinkedList<Integer>();
					values.add(Integer.parseInt(line[0]));
					hintMap.put(hint, values);
				}
			}
		}

		String passwordChars = message.getLines().get(0)[2];

		//we know that the password length is passwordChars.size()-1
		//we generate possible sequences when removing one char from the possible passwordChars
		for (char c : passwordChars.toCharArray()) {
			String sequence = passwordChars.replace(Character.toString(c), "");

			Worker.SeqMessage seqMsg = new Worker.SeqMessage();
			seqMsg.setSequence(sequence);
			seqMsg.setMissingChar(c);
			seqMsg.setHints(hintMap);

			if(this.idleWorkers.size() != 0){
				this.idleWorkers.element().tell(seqMsg, this.self());
				this.busyWorkers.put(this.idleWorkers.remove(), seqMsg);
			}
			else {
				this.unassignedWork.add(seqMsg);
			}
		}

		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());

		idleWorkers.add(this.sender());

		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());

//		this.log().info("Unregistered {}", message.getActor());
	}
}
