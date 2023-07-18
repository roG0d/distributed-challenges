use serde::{Serialize, Deserialize};
use anyhow::{Context, bail};
use std::io::{StdoutLock, Write};


// Struct Message
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message  {
    src: String,

    #[serde(rename = "dest")]
    dst: String,

    body: Body,
}

// Struct Body because of its complexity
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Body {

    #[serde(rename = "msg_id")]
    id: Option<usize>,

    in_reply_to: Option<usize>,
    
    // Needed to flatten the resulting JSON
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
// Serde decorator to call Payload as type
#[serde(tag="type")]
// Serde decorator to convert every Enum payload into snake_cases
#[serde(rename_all = "snake_case")]
enum Payload {
    Init{node_id: String, node_ids:Vec<String>},
    InitOk{},
    Echo {echo: String},
    EchoOk {echo: String},

}

struct EchoNode{
    id:usize,
}

impl EchoNode{

    // fn step to act at any given message depending on its payload
    pub fn step<'a> (
        &mut self, 
        input: Message, 
        output: &mut StdoutLock
    ) -> anyhow::Result<()>{
        
        match input.body.payload {
            Payload::Init{ .. } => {
                let reply =  Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk { },
                    },
                };

                // Serialize the rust struct into a json object with context in case of fail
                serde_json::to_writer(&mut *output, &reply).context("serialize response to init")?;
                let _ = output.write_all(b"\n").context("Write trailing newline");

                self.id += 1;

            }       
            Payload::InitOk { .. } =>  bail!("can't have an InitOk reply"),
            Payload::Echo { echo } =>{

                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };

                // Serialize the rust struct into a json object with context in case of fail
                serde_json::to_writer(&mut *output, &reply).context("serialize response to echo")?;
                let _ = output.write_all(b"\n").context("Write trailing newline");
                self.id += 1;
            }
            Payload::EchoOk { .. }  => {} 
        }
            Ok(())
        }
      
}

fn main() -> anyhow::Result<()>{

    // Lock the stdin for the thread 
    let stdin = std::io::stdin().lock();

    // Deserizalize json into Message structs creating an iterator
    let inputs  = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    // Lock the stdout for the thread
    let mut stdout = std::io::stdout().lock();

    // initialization of echonodes
    let mut state = EchoNode {
        id: 0,
    };

    // For every input:
    for input in inputs {
        let input = input.context("Malestrom input from STDIN could not be deserialized")?;

        // Take the step fn 
        state.step(input, &mut stdout).context("Node step function failed")?;
    }

    Ok(())
}

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target too)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/rustengan --node-count 1 --time-limit 10