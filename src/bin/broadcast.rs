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
    Broadcast{ 
        #[serde(rename="message")]
        value: usize
    },
    BroadcastOk{},

    Read{},
    ReadOk{
        #[serde(rename="messages")]
        values: Vec<usize>,
    },
}

struct BroadcastNode{
    id:usize,
    values: Vec<usize>,
}

// Implementation of the trait Node for EchoNode
impl Node<(), Payload> for BroadcastNode{
    
    fn from_init(state: (), init: Init) -> anyhow::Result<Self> 
        where 
            Self:Sized {
        Ok(BroadcastNode { id: 1, values: Vec::new() })
    }

    // fn step to act at any given message depending on its payload
    fn step<'a> (
        &mut self, 
        input: Message<Payload>, 
        output: &mut StdoutLock
    ) -> anyhow::Result<()>{
        
        match input.body.payload {
            Payload::Broadcast { value } =>{
                self.values.push(value);
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::BroadcastOk {},
                    },
                };

                // Serialize the rust struct into a json object with context in case of fail
                serde_json::to_writer(&mut *output, &reply).context("serialize response to broadcast")?;
                let _ = output.write_all(b"\n").context("Write trailing newline");
                self.id += 1;
            }
            Payload::BroadcastOk { .. }  => {}

            Payload::Read { .. } => {
                let values = self.values.clone();
                let reply =  Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::ReadOk { values },
                    },
                };

                // Serialize the rust struct into a json object with context in case of fail
                serde_json::to_writer(&mut *output, &reply).context("serialize response to rpc")?;
                let _ = output.write_all(b"\n").context("Write trailing newline");

                self.id += 1;

            }
            Payload::ReadOk { .. } => {}
        }
            Ok(())
        }
      
}

fn main() -> anyhow::Result<()>{

    //We call the main_loop function with a initial state (as we had the trait implemented for EchoNode)
    let _ = main_loop::<_, BroadcastNode, _>(());
    Ok(())
 }

// command to run malestron broadcast test, has to be on maelstrom file where maelstrom exe is (have to indicate the rust compilation target too)
// ./maelstrom test -w broadcast --bin ../../rustengan/target/debug/broadcast --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
