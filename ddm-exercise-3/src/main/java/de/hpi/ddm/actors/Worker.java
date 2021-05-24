package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import de.hpi.ddm.actors.Master.TaskMessage;
import de.hpi.ddm.systems.MasterSystem;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
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
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	/*@Data @NoArgsConstructor @AllArgsConstructor
	public static class WelcomeMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private BloomFilter welcomeData;
	}*/
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CompletionMessage implements Serializable {
		private static final long serialVersionUID = 2333143952648649095L;
		private String password;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private final ActorRef largeMessageProxy;

	private char[] passwordAlphabet;
	private int passwordLength;
	private boolean passwordPropertiesDetermined = false;

	private String passwordToCrack;
	private String hashedPassword;
	private List<Character> crackedHints;
	private String[] hintHashes;
	private String crackedPassword = "";
	private int numberOfHints;
	
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
				.match(Master.TaskMessage.class, this::handle)
				// TODO: Add further messages here to share work between Master and Worker actors
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

	private void handle(TaskMessage task) {
		//0. Pre-Processing
		String[] line = task.getLine();

		if (!passwordPropertiesDetermined) {

			this.passwordAlphabet = line[2].toCharArray();
			this.hashedPassword = line[3];
			this.passwordLength = Integer.parseInt(line[3]);
			this.passwordToCrack = line[4];
		}

		this.numberOfHints = line.length - 5;
		this.hintHashes =  Arrays.copyOfRange(line, 5, line.length);
		this.crackedHints = new ArrayList<>();
		
		//1. Hint cracking

		this.heapPermutation(this.passwordAlphabet, this.passwordLength);
		// difference between cracked hints & password alphabet -> password characters

		Set<Character> impossiblePasswordCharacters = new HashSet(crackedHints);
		Set<Character> passwordCharacters = new HashSet(Collections.singleton(passwordAlphabet));
		passwordCharacters.removeAll(impossiblePasswordCharacters);

		//2. PW cracking
		this.printAllKLength(Arrays.copyOf(passwordCharacters.toArray(), passwordCharacters.size(), Character[].class) ,this.passwordLength);

		//3. Send PW to Master
		CompletionMessage completed = new CompletionMessage(this.crackedPassword);
		this.sender().tell(completed, this.self());

		
	}

	private String hash(String characters) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes(StandardCharsets.UTF_8));
			
			StringBuilder stringBuffer = new StringBuilder();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	private void checkPermutation(String permutation){
		char impossiblePasswordCharacter = permutation.charAt(0);
		String possibleHintHash = hash(permutation.substring(1, permutation.length()));
		for(String hint:this.hintHashes){
			if(possibleHintHash.equals(hint)) {
				this.crackedHints.add(impossiblePasswordCharacter);
				break;
			}
		}
	}

	private void checkPassword(String possiblePassword) {
		String possiblePasswordHash = hash(possiblePassword);
		if(possiblePasswordHash.equals(hashedPassword)){
			System.out.println("Found the password!");
			this.crackedPassword = possiblePassword;
			return;
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			this.checkPermutation(new String(a));
			if (this.crackedHints.size() >= this.hintHashes.length) {
				return;
			}

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1);

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

	private void printAllKLength(Character[] set, int k) {
		int n = set.length;
		printAllKLengthRec(set, "", n, k);
		return;
	}
	
	// The main recursive method
	// to print all possible
	// strings of length k
	private void printAllKLengthRec(Character[] set, String prefix, int n, int k)	{
		if (!this.crackedPassword.equals("")) {
			return;
		}
		// Base case: k is 0,
		// print prefix
		if (k == 0)	{
			checkPassword(prefix);
			return;
		}

		// One by one add all characters
		// from set and recursively
		// call for k equals to k-1
		for (int i = 0; i < n; ++i){
			// Next character of input added
			String newPrefix = prefix + set[i];

			// k is decreased, because
			// we have added a new character
			printAllKLengthRec(set, newPrefix, n, k - 1);
		}
	}
}