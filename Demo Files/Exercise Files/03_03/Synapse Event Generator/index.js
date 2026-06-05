const readline = require("readline");
const { EventHubProducerClient } = require("@azure/event-hubs");
var Chance = require('chance');
const fsExtra = require('fs-extra');
const { Console } = require("console");

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});
var connection_string = "" ;
var hub_name="";
rl.question("Enter Connection String:    ", (cs) => {
    
    connection_string=cs;
    if(!connection_string){
        console.log('Connection string is required');
        rl.close();
        return;
    }
  
    rl.question("Enter hub name:    ", (hubName)=>{
        hub_name=hubName;
        if(!hub_name){
            console.log('Hub Name is required');
            rl.close();
            return;
        }
        else{
            rl.close();

            main().then(()=>{
                console.log('Finished')
            
                }).catch(err=>console.log(err))
        }
    })


   

});
async function main() {
    let max_events=100000;
    var chance = new Chance();
    const producer = new EventHubProducerClient(connection_string, hub_name);
    var stores= fsExtra.readJSONSync('stores.json')

    console.log('Entries to be generated:::::> ' + max_events)
    console.log('Batch Size:::::> ' + stores.length)
    let generated_entries=0;
    while(generated_entries<max_events){
        const batch = await producer.createBatch();
        stores.forEach(store => {
            let selfCheckout= chance.bool();
            batch.tryAdd({
                body:{
                     StoreCode: store.code,
                     Date: new Date(),
                     Duration:chance.integer({ min: 1000 }),
                     Operator : selfCheckout? '' : chance.ssn( ),
                     SelfCheckout:selfCheckout,
                     CashOnHand: selfCheckout? false: chance.bool()
                    }
            })
        });

        await producer.sendBatch(batch);
       generated_entries += stores.length;
       console.log(`Total Entries Sent:::::> ${generated_entries}`)
    }



  // Close the producer client.
  await producer.close();
  console.log('Finished')

}


