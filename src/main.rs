
//TODO:Handle requests and stream data 
use std::{env, error::Error, process,fmt};
use csv;
use serde::{Deserialize};
use std::collections::HashMap;


#[derive(Debug,Deserialize)]
struct Transaction{
    tx_type:String,
    client:u16,
    tx:u16,
    amount:f32
}

#[derive(Debug)]
struct Client{
    _id:u16,
    total:f32,
    held:f32,
    available:f32,
    locked:bool,
    txs:HashMap<u16,f32>,
    dispute_tx:HashMap<u16,f32>,
}

impl fmt::Display for Client {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Write strictly the first element into the supplied output
        // stream: `f`. Returns `fmt::Result` which indicates whether the
        // operation succeeded or failed. Note that `write!` uses syntax which
        // is very similar to `println!`.
        write!(f, "{},{:.4},{:.4},{:.4},{}", self._id,self.total,self.held,self.available,self.locked)
    }
}


fn deposit(chm:&mut HashMap<u16,Client>,clientid:u16,amount:f32,txid:u16){
    let tmp=chm.get_mut(&clientid).unwrap();
    if tmp.locked{
        return
    }
    tmp.total+=amount;
    tmp.available=tmp.total-tmp.held;
    tmp.txs.insert(txid, amount);
}

fn withdrawal(chm:&mut HashMap<u16,Client>,clientid:u16,amount:f32,txid:u16){
    let tmp=chm.get_mut(&clientid).unwrap();
    if tmp.locked || tmp.available < amount  {
        return
    }
    tmp.total-=amount;
    tmp.available=tmp.total-tmp.held;
    tmp.txs.insert(txid, amount);
}
fn dispute(chm:&mut HashMap<u16,Client>,tx_id:u16,clientid:u16){

    let tmp=chm.get_mut(&clientid).unwrap();
    if tmp.locked{
        return
    };
    
    if let Some(amnt) = tmp.txs.get(&tx_id) {
        tmp.dispute_tx.insert(tx_id, *amnt); 
        tmp.held+=*amnt;
        tmp.available=tmp.total-tmp.held;   
    }else{
        return
    }

}
fn resolve(chm:&mut HashMap<u16,Client>,tx_id:u16,clientid:u16){
    let tmp=chm.get_mut(&clientid).unwrap();
    if tmp.locked{
        return
    };
    if tmp.dispute_tx.contains_key(&tx_id){
        tmp.held-=tmp.dispute_tx.get(&tx_id).unwrap();
        tmp.available=tmp.total-tmp.held;
    }else{
        return
    }
    tmp.dispute_tx.remove(&tx_id);
}
fn chargeback(chm:&mut HashMap<u16,Client>,tx_id:u16,clientid:u16){
    let tmp=chm.get_mut(&clientid).unwrap();
    if tmp.locked{
        return
    };
    if tmp.dispute_tx.contains_key(&tx_id) {
        let funds=tmp.dispute_tx.get(&tx_id).unwrap();
        tmp.total-=funds;
        tmp.held-=funds;
        tmp.available=tmp.total-tmp.held;
        tmp.locked=true;
    }else{return ;}
}

fn process_txs(file_path:&String) -> Result<(), Box<dyn Error>> {
    
    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::Reader::from_path(file_path)?;
    
    let mut clients: HashMap<u16,Client> = HashMap::new();
    
    for result in rdr.deserialize() {
     
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record:Transaction = result?;

        if !clients.contains_key(&record.client){
            clients.insert(record.client, Client{
                _id:record.client,
                total:0.0,
                held:0.0,
                available:0.0,
                locked:false,
                txs:HashMap::new(),
                dispute_tx:HashMap::new(),
            });
            
        }
            
        match record.tx_type.as_str() {
                "deposit"=>deposit(&mut clients, record.client, record.amount, record.tx),
                "withdrawal"=>withdrawal(&mut clients, record.client, record.amount, record.tx),
                "dispute"=>dispute(&mut clients, record.tx,record.client),
                "resolve"=>resolve(&mut clients, record.tx,record.client),
                "chargeback"=>chargeback(&mut clients, record.tx,record.client),
                _=>()
            }

    }
    println!("_id,total,held,available,locked");
    for (_,v) in clients.iter(){
        println!("{:}",v);
    }
    Ok(())
}


fn main() {
    
    let args:Vec<String>=env::args().collect();

    let file_path:&String=&args[1];

    if let Err(err) = process_txs(file_path) {
        println!("error running example: {}", err);
        process::exit(1);
    }
}
