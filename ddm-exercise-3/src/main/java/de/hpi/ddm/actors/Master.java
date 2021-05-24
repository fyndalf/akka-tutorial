package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

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

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
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
	
	@Data @AllArgsConstructor @NoArgsConstructor
	public static class TaskMessage implements Serializable {
		private static final long serialVersionUID = 8263091671855768242L;
		private String[] line;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final ActorRef largeMessageProxy;

	private final Queue<ActorRef> idleWorkers = new LinkedBlockingQueue<>();
	private final Queue<TaskMessage> taskMessages = new LinkedBlockingQueue<>();
	private final HashMap<ActorRef, TaskMessage> workerTaskAssignment = new HashMap<>();
	private int passwordsInQueueCounter = 0;

	private long startTime;
	
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
				.match(Worker.CompletionMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void handle(BatchMessage message) {
		
		// - The Master received the first batch of input records.
		// - To receive the next batch, we need to send another ReadMessage to the reader.
		// - If the received BatchMessage is empty, we have seen all data for this task.
		// - We need a clever protocol that forms sub-tasks from the seen records, distributes the tasks to the known workers and manages the results.
		//   -> Additional messages, maybe additional actors, code that solves the subtasks, ...
		//   -> The code in this handle function needs to be re-written.
		// - Once the entire processing is done, this.terminate() needs to be called.
		
		// Info: Why is the input file read in batches?
		// a) Latency hiding: The Reader is implemented such that it reads the next batch of data from disk while at the same time the requester of the current batch processes this batch.
		// b) Memory reduction: If the batches are processed sequentially, the memory consumption can be kept constant; if the entire input is read into main memory, the memory consumption scales at least linearly with the input size.
		// - It is your choice, how and if you want to make use of the batched inputs. Simply aggregate all batches in the Master and start the processing afterwards, if you wish.

		// Stop fetching lines from the Reader once an empty BatchMessage was received; we have seen all data then
		if (message.getLines().isEmpty()) {
			log().info("Latest log chunk was empty, stop fetching new chunks.");
			return;
		}

		log().info("Got log chunk, adding {} tasks", message.getLines().size());
		passwordsInQueueCounter += message.getLines().size();

		for (String[] line : message.getLines()){
			this.taskMessages.add(new TaskMessage(line));
		}

		while (!this.idleWorkers.isEmpty() && !this.taskMessages.isEmpty()) {
			assignAvailableTaskToWorker();
		}

		this.reader.tell(new Reader.ReadMessage(), this.self());

	}

	private void assignAvailableTaskToWorker() {
		ActorRef availableWorker = this.idleWorkers.remove();
		assignAvailableTaskToWorker(availableWorker);
	}

	private void assignAvailableTaskToWorker(ActorRef worker) {
		TaskMessage task = this.taskMessages.remove();
		worker.tell(task, this.self());
		log().info("Sent task to worker {}", worker);
		workerTaskAssignment.put(worker, task);
	}
	
	protected void terminate() {
		this.collector.tell(new Collector.PrintMessage(), this.self());
		
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
		this.log().info("Registered {}", this.sender());
		
		//todo: use large message proxy to send task data
		//this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(new Worker.WelcomeMessage(this.welcomeData), this.sender()), this.self());
		
		if (!this.taskMessages.isEmpty()) {
			assignAvailableTaskToWorker(this.sender());
		} else {
			this.idleWorkers.add(this.sender());
		}
	
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
		TaskMessage cancelledTask = workerTaskAssignment.get(message.getActor());
		if (cancelledTask != null){
			this.taskMessages.add(cancelledTask);
			while (!this.idleWorkers.isEmpty() && !this.taskMessages.isEmpty()) {
				assignAvailableTaskToWorker();
			}
		}
	}

	protected void handle(Worker.CompletionMessage message) {
		this.idleWorkers.add(this.sender());
		passwordsInQueueCounter	--;
		workerTaskAssignment.remove(sender());
		log().info("Got password from worker {}", this.sender());
		this.collector.tell(new Collector.CollectMessage(message.getResult()), this.self());
		// we have cracked all passwords and can terminate the cracking
		if (passwordsInQueueCounter <= 0 && this.taskMessages.isEmpty()) {
			log().info("Received last password, terminating execution and printing results");
			this.terminate();
		} else {
			if (!this.idleWorkers.isEmpty()  && !this.taskMessages.isEmpty()) {
				assignAvailableTaskToWorker(this.sender());
			} else {
				this.idleWorkers.add(this.sender());
			}
		}
	}
}
