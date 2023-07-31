use rustengan::*;
use serde::{Serialize, Deserialize};
use anyhow::Context;
use std::{io::{StdoutLock, Write}, collections::HashMap};

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
    
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk{},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BroadcastNode{
    node: String,
    id: usize,
    values: Vec<usize>,
}

// Implementation of the trait Node for EchoNode
impl Node<(), Payload> for BroadcastNode{
    fn from_init(_state: (), init: Init) -> anyhow::Result<Self> 
        where 
            Self:Sized {
        Ok(BroadcastNode { node:init.node_id, id: 1, values: Vec::new() })
    }

    // fn step to act at any given message depending on its payload
    fn step<'a> (
        &mut self, 
        input: Message<Payload>, 
        output: &mut StdoutLock
    ) -> anyhow::Result<()>{
        
        let mut reply = input.into_reply(Some(&mut self.id));

        match reply.body.payload {
            Payload::Broadcast { value } =>{
                self.values.push(value);
                reply.body.payload = Payload::BroadcastOk{};
                // Serialize the rust struct into a json object with context in case of fail
                serde_json::to_writer(&mut *output, &reply).context("serialize response to broadcast")?;
                let _ = output.write_all(b"\n").context("Write trailing newline");
            }
            

            Payload::Read{..}  => {
                reply.body.payload = Payload::ReadOk{
                    values: self.values.clone(),
                };
                // Serialize the rust struct into a json object with context in case of fail
                serde_json::to_writer(&mut *output, &reply).context("serialize response to read")?;
                let _ = output.write_all(b"\n").context("Write trailing newline");

            }
            

            Payload::Topology { topology} => { 
                    reply.body.payload = Payload::TopologyOk{};
                    // Serialize the rust struct into a json object with context in case of fail
                    serde_json::to_writer(&mut *output, &reply).context("serialize response to topology")?;
                    let _ = output.write_all(b"\n").context("Write trailing newline");

            },

            // A way to group up different matches with the same handler
            Payload::BroadcastOk { .. }  | Payload::ReadOk { .. } | Payload::TopologyOk {  } => {},
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
// ./maelstrom test -w broadcast --bin ../../rustengan/target/debug/broadcast --node-count 1 --time-limit 20 --rate 10

