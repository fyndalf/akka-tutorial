package de.hpi.ddm.actors;

import java.io.Serializable;
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

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CompletionMessage implements Serializable {
		private static final long serialVersionUID = 2333143952648649095L;
		private String result;
		private String resultId;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private final ActorRef largeMessageProxy;

	private List<Character> crackedHints = new ArrayList<>();
	private List<String> hintHashes;
	private String crackedPassword = "";

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
		log().info("Got task - 0/3");
		String[] line = task.getLine();
		char[] globalAlphabet = Arrays.copyOf(line[2].toCharArray(), line[2].toCharArray().length);
		int passwordLength = Integer.parseInt(line[3]);
		String passwordToCrack = line[4];
		String passwordId = line[0];

		this.hintHashes = new ArrayList(Arrays.asList(Arrays.copyOfRange(line, 5, line.length)));
		this.crackedHints = new ArrayList<>();
		this.crackedPassword = "";

		//1. Hint cracking
		log().info("Start cracking hints - 1/3");
		generatePermutation(globalAlphabet, globalAlphabet.length);
		List<Character> passwordAlphabet = new ArrayList<>();
		for (char character : globalAlphabet) {
			if (!this.crackedHints.contains(character)) {
				passwordAlphabet.add(character);
			}
		}

		//2. PW cracking
		log().info("Start cracking password - 2/3");
		generateCombinations(passwordAlphabet, "", passwordAlphabet.size(), passwordLength, passwordToCrack);

		//3. Send PW to Master
		log().info("Send to master - 3/3");
		CompletionMessage completed = new CompletionMessage(this.crackedPassword, passwordId);
		this.sender().tell(completed, this.self());
	}

	private String hash(String characters) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes(StandardCharsets.UTF_8));

			StringBuilder stringBuffer = new StringBuilder();
			for (byte hashedByte : hashedBytes) {
				stringBuffer.append(Integer.toString((hashedByte & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	private void checkPermutation(String permutation){
		char impossiblePasswordCharacter = permutation.charAt(permutation.length() - 1);
		String possibleHintHash = hash(permutation.substring(0, permutation.length() - 1));
		for (String hint:this.hintHashes) {
			if (possibleHintHash.equals(hint)) {
				this.hintHashes.remove(hint);
				this.crackedHints.add(impossiblePasswordCharacter);
				break;
			}
		}
	}

	private void generatePermutation(char[] a, int size) {
		// If no more permutations needed, exit the algorithm
		if (this.crackedHints.size() >= this.hintHashes.size()) {
			return;
		}

		// If size is 1, store the obtained permutation
		if (size == 1) {
			this.checkPermutation(new String(a));
			// if we have cracked all hints, we no longer need to search.
			if (this.crackedHints.size() >= this.hintHashes.size()) {
				return;
			}
		}

		for (int i = 0; i < size; i++) {
			generatePermutation(a, size - 1);
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}

	private void generateCombinations(List<Character> set, String prefix, int n, int k, String passwordHash)  {
		if (!this.crackedPassword.equals("")) {
			return;
		}
		if (k == 0) {
			if (hash(prefix).equals(passwordHash)) {
				this.crackedPassword = prefix;
			}
			return;
		}
		for (int i = 0; i < n; i++) {
			String newPrefix = (prefix + set.get(i));
			generateCombinations(set, newPrefix, n, k - 1, passwordHash);
		}
	}

}