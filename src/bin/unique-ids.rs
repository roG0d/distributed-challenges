use rustengan::*;
use serde::{Serialize, Deserialize};
use anyhow::{Context, bail};
use ulid::Ulid;
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
// Serde decorator to call Payload as type
#[serde(tag="type")]
// Serde decorator to convert every Enum payload into snake_cases
#[serde(rename_all = "snake_case")]
enum Payload {
    Init{node_id: String, node_ids:Vec<String>},
    InitOk{},
    Generate{},
    GenerateOk{
        #[serde(rename="id")]
        guid: String}

}

struct UniqueNode{
    id:usize,
}

// Implementation of the trait Node for EchoNode
impl Node<Payload> for UniqueNode{

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
            Payload::Generate { } =>{
                let guid = Ulid::new().to_string();
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::GenerateOk { guid },
                    },
                };

                // Serialize the rust struct into a json object with context in case of fail
                serde_json::to_writer(&mut *output, &reply).context("serialize response to generate")?;
                let _ = output.write_all(b"\n").context("Write trailing newline");
                self.id += 1;
            }
            Payload::GenerateOk { .. }  => {} 
        }
            Ok(())
        }
      
}

fn main() -> anyhow::Result<()>{

    //We call the main_loop function with a initial state (as we had the trait implemented for EchoNode)
    let _ = main_loop(UniqueNode{ id : 0});
    Ok(())
 }

// command to run malestron uinque-ids test, has to be on maelstrom file where maelstrom exe is (have to indicate the rust compilation target too)
// ./maelstrom test -w unique-ids --bin ../../rustengan/target/debug/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
