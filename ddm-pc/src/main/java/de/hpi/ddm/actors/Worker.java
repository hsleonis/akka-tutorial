package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class OldMessage implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private int passwordLength;
		//private LinkedList<String> passwordChars;
		//private LinkedList<String> passwords;
		private String passwordChars;
		private String password;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class SeqMessage implements Serializable {
		private static final long serialVersionUID = 7647246076267640540L;
		private String sequence;
		private HashMap<String, LinkedList<Integer>> hints;
		private char missingChar;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;

	private HashSet<String> permutations = new HashSet<String>();
	private HashMap<String, String> possiblePasswords = new HashMap<String, String>();

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(OldMessage.class, this::handle)
				.match(SeqMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private void handle(SeqMessage message) {
		this.permutations.clear();

		char[] sequence = message.sequence.toCharArray();
		heapPermutation(sequence, sequence.length, sequence.length, this.permutations, message.hints, message.missingChar);

		Master.HintsCompletedMessage msg = new Master.HintsCompletedMessage();
		msg.setMissingChar(message.missingChar);
		this.sender().tell(msg, this.self());
	}

	private void handle(OldMessage message) {
		//all letters that are left -> generate all possible strings, hash them, compare with password
		char[] set = message.passwordChars.toCharArray();
		int k = message.passwordLength;
		int n = set.length;
		this.possiblePasswords.clear();
		possibleKStrings(set, "", n, k, possiblePasswords);
		if(possiblePasswords.containsKey(message.password)) {
			//return password in password message
			Master.PasswordMessage msg = new Master.PasswordMessage();
			msg.setResult(possiblePasswords.get(message.password));
			this.sender().tell(msg, this.self());
			return;
		}
		System.out.println("No password found!");
	}
	
	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// the permutations are hashed and compared to the hints
	// sends a message to the master if a hint is decrypted
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, int n, HashSet<String> l, HashMap<String, LinkedList<Integer>> hints, char missingChar) {
		// If size is 1, store the obtained permutation
		if (size == 1){
			String sequence = hash(String.valueOf(a));

			if(hints.containsKey(sequence)) {
				LinkedList<Integer> values = hints.get(sequence);
				for (int index : values) {
					Master.PasswordCharMessage msg = new Master.PasswordCharMessage();
					msg.setPasswordIndex(index);
					msg.setMissingChar(missingChar);
					this.sender().tell(msg, this.self());
				}
			}
			l.add(sequence);
		}

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l, hints, missingChar);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}

	// Generating all possible strings of length k
	// strings are hashed
	// https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
	private void possibleKStrings(char[] set, String prefix, int n, int k, HashMap<String, String> l)
	{
		// Base case: k is 0, store prefix
		if (k == 0)
		{
			l.put(hash(prefix), prefix);
			return;
		}

		// One by one add all characters from set and recursively call for k equals to k-1
		for (int i = 0; i < n; ++i)
		{
			// Next character of input added
			String newPrefix = prefix + set[i];
			// k is decreased, because we have added a new character
			possibleKStrings(set, newPrefix, n, k - 1, l);
		}
	}
}