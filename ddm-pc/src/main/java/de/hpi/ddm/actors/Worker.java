package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;

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
	public static class HintMessage implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private String[] hints;
		private int passwordLength;
		private String passwordChars;
		private ActorRef sender;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private LinkedList<String> encryptedHints = new LinkedList();
	
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
				.match(HintMessage.class, this::handle)
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

	private void handle(HintMessage message) {
		System.out.println(message.getHints());

		//generate sequences, hash them and compare with hints
		//all letters that are left -> print all possible strings, hash them, compare with password
		//return password in password message

		//example: Generating all permutations of an array
		String allChars = message.getPasswordChars();
		List<char[]> possibleChars = new LinkedList();

		//int j = 0;
		for(int j = 0; j < allChars.length(); j++){
			char[] arr  = new char[allChars.length() - 1];
			for (int i = 0, k = 0; i < allChars.length(); i++) {
				if (i == j) {
					continue;
				}
				arr[k++] = allChars.charAt(i);
			}
			possibleChars.add(arr);
		}

		System.out.println("num hints: " + message.getHints().length);

		//char[] passwortSet;

		// in the possibleChars array the chars are missing in the order of the passwordChars attribute
		for(int i = 0; i < possibleChars.size(); i++) {
		//for(char[] array : possibleChars) {
			char[] array = possibleChars.get(i);
			List<String> sequences = new LinkedList<String>();
			heapPermutation(array, array.length, array.length, sequences);
			boolean foundHint = encryptHint(message.getHints(), sequences);
			if(foundHint == false){
				System.out.println(allChars.charAt(i) + " is missing!");
			}
		}



		/*
		//example: Generating all possible strings of length k
		char[] set = {'a', 'b'};
		int k = 3;
		int n = set.length;
		List<String> l = new LinkedList<String>();
		printAllKLengthRec(set, "", n, k, l);
		System.out.println(l);
		 */

		Master.PasswordMessage msg = new Master.PasswordMessage();
		msg.setResult(message.getPasswordChars());
		message.getSender().tell(msg, this.self());

	}

	private boolean encryptHint(String[] hints, List<String> sequences) {
		for (String word : sequences) {
			for (String hint : hints) {
				if(hash(word).equals(hint)){
					encryptedHints.add(word);
					System.out.println(encryptedHints.size() + " -- " + this.self());
					return true;
					/* if(encryptedHints.size() == message.getHints().length) {
						return;
					}
					break;

					 */
				}
			}
		}
		return false;
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
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, int n, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l);

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
	// https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
	private void printAllKLengthRec(char[] set, String prefix, int n, int k, List<String> l)
	{
		// Base case: k is 0, store prefix
		if (k == 0)
		{
			l.add(prefix);
			return;
		}

		// One by one add all characters from set and recursively call for k equals to k-1
		for (int i = 0; i < n; ++i)
		{
			// Next character of input added
			String newPrefix = prefix + set[i];
			// k is decreased, because we have added a new character
			printAllKLengthRec(set, newPrefix, n, k - 1, l);
		}
	}
}