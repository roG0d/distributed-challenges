use rustengan::*;
use serde::{Serialize, Deserialize};
use anyhow::{Context, bail};
use std::io::{StdoutLock, Write};

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

// Implementation of the trait Node for EchoNode
impl Node<Payload> for EchoNode{

    // fn step to act at any given message depending on its payload
    fn step<'a> (
        &mut self, 
        input: Message<Payload>, 
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

    //We call the main_loop function with a initial state (as we had the trait implemented for EchoNode)
    let _ = main_loop(EchoNode{ id : 0});
    Ok(())
 }

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target tooz)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/rustengan --node-count 1 --time-limit 10