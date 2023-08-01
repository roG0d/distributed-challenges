use rustengan::*;
use serde::{Serialize, Deserialize};
use anyhow::Context;
use std::{io::{StdoutLock, Write, StderrLock}, collections::HashMap};

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
    neighbors: HashMap<String, Vec<String>>,
}

// Implementation of the trait Node for EchoNode
impl Node<(), Payload> for BroadcastNode{
    fn from_init(_state: (), init: Init) -> anyhow::Result<Self> 
        where 
            Self:Sized {
        Ok(BroadcastNode { node:init.node_id, id: 1, values: Vec::new(), neighbors: HashMap::new()})
    }

    // fn step to act at any given message depending on its payload
    fn step<'a> (
        &mut self, 
        input: Message<Payload>, 
        output: &mut StdoutLock
    ) -> anyhow::Result<()>{
        
        let mut reply = input.clone().into_reply(Some(&mut self.id));

        match reply.body.payload {
            Payload::Broadcast { value } =>{
                
                reply.body.payload = Payload::BroadcastOk{};
                // Serialize the rust struct into a json object with context in case of fail
                serde_json::to_writer(&mut *output, &reply).context("serialize response to broadcast")?;
                let _ = output.write_all(b"\n").context("Write trailing newline");
                
                // Checking if the node already has received a broadcast message with the same value (So it dont loop infinite with that broadcast msg)
                if !self.values.contains(&value){
                    let own_neighbors = self.neighbors.get(&self.node);
                    match own_neighbors{
                        Some(subnodes) => {
                        
                        // If not, A broadcast message is created for each neighbor of the actual node
                            for neighbor in subnodes {
                                let broadcast_msg = Message::<Payload> {
                                    src: input.dst.clone(),
                                    dst: neighbor.clone(),
                                    body: Body {
                                        id: Some(&mut self.id).map(|id| {
                                            let mid = *id;
                                            *id += 1;
                                            mid
                                    }),
                                        in_reply_to: input.body.id,
                                        payload: Payload::Broadcast { value },
                                    }
                                };
                                
                                serde_json::to_writer(&mut *output, &broadcast_msg).context("serialize response to broadcast")?;
                                let _ = output.write_all(b"\n").context("Write trailing newline");
                            }
                        // The actual value is pushed to stop a next iteration of the loop for that broadcast msg
                        self.values.push(value);

                        },
                        None => {}
                    }
                }
                
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
                    self.neighbors = topology.clone();
                    // printing neighbors:
                    //eprintln!("My neighbors are: {:?}", self.neighbors);
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
// Single-node broadcast test command: 
// ./maelstrom test -w broadcast --bin ../../rustengan/target/debug/broadcast --node-count 1 --time-limit 20 --rate 10

//Testing neighbors command:
//./maelstrom test -w broadcast --bin ../../rustengan/target/debug/broadcast --time-limit 5 --log-stderr

//Multi-node broadcast test command: 
//./maelstrom test -w broadcast --bin ../../rustengan/target/debug/broadcast --node-count 5 --time-limit 20 --rate 10
