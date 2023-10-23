//Lib.rs contains all common code for the challenges to execute
use anyhow::Context;
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    any::TypeId,
    future::Future,
    io::{stdin, BufRead, StdoutLock, Write},
    sync::Arc, time::Duration,
};
use tokio::{
    io::{AsyncWriteExt, Stdout},
    sync::Mutex,
    task::{self, JoinHandle}, time::sleep,
};

// Struct Message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

// Implementation of the message for a generic type of payload
impl<Payload> Message<Payload> {
    pub fn into_reply(self, id: Option<&mut usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: id.map(|id| {
                    let mid = *id;
                    *id += 1;
                    mid
                }),
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }

    // Send method to reply for different messages
    pub async fn send<'a>(&self, output: &'a mut Arc<Mutex<Stdout>>) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let output_clone = Arc::clone(output);
        let mut output_lock = output_clone.lock().await;

        output_lock
            .write(&serde_json::to_vec(self).expect("Cannot convert to bytes"))
            .await?;
        output_lock.write(b"\n").await?;
        Ok(())
    }
}

// Body struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

// InitPayload struct
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

// Init struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

//Definition of trait Node
#[async_trait]
pub trait Node<S, Payload> {
    // Init for the specific node
    async fn from_init<'a>(state: S, init: Init) -> anyhow::Result<Self>
    where
        Self: Sized;

    // Protocol for the specific node

    async fn step<'a>(
        &mut self,
        input: Message<Payload>,
        output: &'a mut Arc<Mutex<Stdout>>,
    ) -> anyhow::Result<()>;

    async fn gossip<'a>(&mut self, output: &'a mut Arc<Mutex<Stdout>>){
    }
}

/*
Main loop of the program, where all the communication happens. It starts with the creation of a channel to receive events from STDIN.
To alternate between events threads will be created to read from STDIN and then send messages to a channel which will indicate which
tasks to perform: interchanging protocols and gossiping.
  */
#[tokio::main]
pub async fn main_loop<S, N, P>(init_state: S) -> anyhow::Result<()>
where
    P: DeserializeOwned + Send + 'static,
    N: Node<S, P> + Send + 'static,
{
    // Init phase

    // Lock the stdin for the init messages
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();

    // Create the async stdout
    let stdout = tokio::io::stdout();
    let stdout = Arc::new(Mutex::new(stdout));

    // Get the first msg from stdin to check if there's messages
    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("no init message received")
            .context("failed to read init message from stdin")?,
    )
    .context("init message could not be deserialized")?;

    // Check if the first msg it's a init_msg
    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("first message should be init");
    };

    // If so, initialize the node and send a initOk reply
    let node: N = Node::from_init(init_state, init).await?;
    let reply = Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };

    // reply to the init in the async stdout
    let _ = reply.send(&mut stdout.clone()).await;

    // Protocols phase
    let mut tasks = Vec::new();

    // Two thread-safe reference-counting pointer for shared values as the node and the stdout has both async properties and need to be thread-safe if we want to spawn async task
    let node = Arc::new(Mutex::new(node));



    // I THINK WE NEED A LOOP FOR THE GOSSIP SO THE NEMESIS PARTITTION (NO COMMUNICATION IN A CONCRETE TIME WINDOW) CAN BE MANAGED
    //let futere  = node.clone().lock().await.gossip(&mut stdout.clone());
    

    /* CLARIFICATION ARC-MUTEX
    In Rust, values are moved when they are passed to functions or closures, and by default they cannot be used again after being moved unless they implement the Copy trait.
    In this actual code, we are attempting to use stdout inside an asynchronous block within a loop. The async move block will take ownership of stdout during the first iteration
    of the loop, moving it into the block. In subsequent iterations, stdout will no longer be available because it has been moved, hence the error "use of moved value: stdout".

    One common way to address this issue is to use reference counting to share stdout between iterations of the loop.
    The Arc (Atomic Reference Counted) and Mutex (Mutual Exclusion) types from the Rust standard library can be used like this to share safely between iterations:

     */
    // For every line we get from the sync stdin:
    /*NOTE ON STDIN
    Maelstrom will redirect every message nodes write onto stdout to stdin, this was discovered as we introduced gossip messages so the total load of message increased
    significally    
     */
    let mut line_iter  = 0; 
    for line in stdin {
        eprint!("iteration of loop lines nº{}", line_iter);
        eprintln!(" line emited: {}", line.as_ref().expect("no line"));

        line_iter += 1;

        // We clone the Arc (not the stdout and the node themself), which increments the reference count but doesn't duplicate the underlying object.
        let node_clone = Arc::clone(&node);
        let mut stdout_clone = Arc::clone(&stdout);

        // Parsing the stdin lines
        let line = line
            .context("Maelstrom input from STDIN could not be read")
            .expect("Error on STDIN read");
        let input: Message<P> = serde_json::from_str(&line)
            .context("Maelstrom input from STDIN could not be deserialized")
            .expect("Expected message from STDIN");

        /* NOTE ON TOKIO::SPAWN
        Spawning a task enables the task to execute concurrently to other tasks. The spawned task may execute on the current thread, or it may be sent to a different thread to be executed.
        The specifics depend on the current Runtime configuration.

        It is guaranteed that spawn will not synchronously poll the task being spawned. This means that calling spawn while holding a lock does not pose a risk of deadlocking with the spawned task.

        As for these facts, we need that everything inside the spawn block is thread-safe.
         */
        // For every line we spawn async tasks that will perform the protocol readed from stdin. We need to lock shared resources for every tasks in order to avoid concurrency
        
        tasks.push(tokio::spawn(async move {
            // Lock on both the node and the stdout
            let mut node_lock = node_clone.lock().await;
            // A rough wait to manage the gossip load, we will gossip every two lines readed, so we ensure we reduce the total number of gossip message by half
            if line_iter % 2 == 0 {let _ = node_lock.gossip(&mut stdout_clone).await;}
             // Performing the step function for every task
            node_lock.step(input, &mut stdout_clone).await
            
        }));
    }

    // Wait for every task to finish
    for task in tasks {
        let _ = task.await?;
    }

    Ok(())
}

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target tooz)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/rustengan --node-count 1 --time-limit 10

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target tooz)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/rustengan --node-count 1 --time-limit 10

// command to run maelstrom server to interactively see transfer between messages, times, traces, etc.
// ./maelstrom serve
