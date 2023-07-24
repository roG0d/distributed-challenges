
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use anyhow::{Context, bail};
use std::io::{StdoutLock, Write};

// Struct Message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload>  {
    pub src: String,

    #[serde(rename = "dest")]
    pub dst: String,

    pub body: Body<Payload>,
}

// Struct Body, its flattened because of its complexity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {

    #[serde(rename = "msg_id")]
    pub id: Option<usize>,

    pub in_reply_to: Option<usize>,
    
    // Needed to flatten the resulting JSON
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init{
    pub node_id: String, 
    pub node_ids:Vec<String>,
}


//Definition of trait Node
pub trait Node<Payload> {
    fn step<'a> (&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()>;
}

pub fn main_loop<S, Payload>(mut state: S) -> anyhow::Result<()> 
where 
    S: Node<Payload>,
    Payload: DeserializeOwned,
{

    // Lock the stdin for the thread 
    let stdin = std::io::stdin().lock();

    // Deserizalize json into Message structs creating an iterator
    let inputs  = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Payload>>();

    // Lock the stdout for the thread
    let mut stdout = std::io::stdout().lock();

    // For every input:
    for input in inputs {
        let input = input.context("Malestrom input from STDIN could not be deserialized")?;

        // Take the step fn 
        state.step(input, &mut stdout).context("Node step function failed")?;
    }

    Ok(())
}

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target tooz)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/rustengan --node-count 1 --time-limit 10

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target tooz)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/rustengan --node-count 1 --time-limit 10