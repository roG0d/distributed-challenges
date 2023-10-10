//Lib.rs contains all common code for the challenges to execute
use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::{stdin, BufRead, StdoutLock, Write};

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
    pub fn send(&self, output: &mut impl Write) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        serde_json::to_writer(&mut *output, self).context("serialize response message")?;
        let _ = output.write_all(b"\n").context("write trailing newline");
        Ok(())
    }
}

// Messages sent to the Sender channel representing an Event
#[derive(Debug, Clone)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
    EOF,
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
pub trait Node<S, Payload, InjectedPayload = ()> {
    // Init for the specific node
    fn from_init(
        state: S,
        init: Init,
        inject: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    // Protocol for the specific node
    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()>;
}

/* 
Main loop of the program, where all the communication happens. It starts with the creation of a channel to receive events from STDIN.
To alternate between events threads will be created to read from STDIN and then send messages to a channel which will indicate which
tasks to perform: interchanging protocols and gossiping.
  */
pub fn main_loop<S, N, P, IP>(init_state: S) -> anyhow::Result<()>
where
    P: DeserializeOwned + Send + 'static,
    N: Node<S, P, IP>,
    IP: Send + 'static,
{
    // Creating a channel so we can alternate between protocols and gossips
    let (sx, rx) = std::sync::mpsc::channel();

    // Lock the stdin for the init messages
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();

    // Lock the stdout
    let mut stdout = std::io::stdout().lock();

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
    let mut node: N =
        Node::from_init(init_state, init, sx.clone()).context("node initilization failed")?;

    let reply = Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };

    // reply to the init
    serde_json::to_writer(&mut stdout, &reply).context("serialize response to init")?;
    stdout.write_all(b"\n").context("write trailing newline")?;

    // STDIN unlocked
    drop(stdin);

    // Spawn threads that will read from the STDIN. It's an synchronous way to perfom a select over multiple tasks
    let jh = std::thread::spawn(move || {
        // Lock the stdin for every thread
        let stdin = std::io::stdin().lock();

        // this loop its gonna read from STDIN, get a message with an event, and send it to the receiver so then it can be performed
        for line in stdin.lines() {
            let line = line.context("Maelstrom input from STDIN could not be read")?;
            let input: Message<P> = serde_json::from_str(&line)
                .context("Maelstrom input from STDIN could not be deserialized")?;
            if let Err(_) = sx.send(Event::Message(input)) {
                return Ok::<_, anyhow::Error>(()).context("Message could not be sent to the channel");
            }
        }
        let _ = sx.send(Event::EOF);
        Ok(())
    });

    // Iterates the receiver looking for Events and executes step function for every one of them
    for input in rx {
        node.step(input, &mut stdout)
            .context("Node step function failed")?;
    }

    // Wait for every thread to finish
    jh.join()
        .expect("stdin thread panicked")
        .context("stdin thread error")?;

    Ok(())
}

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target tooz)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/rustengan --node-count 1 --time-limit 10

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target tooz)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/rustengan --node-count 1 --time-limit 10

// command to run maelstrom server to interactively see transfer between messages, times, traces, etc.
// ./maelstrom serve
