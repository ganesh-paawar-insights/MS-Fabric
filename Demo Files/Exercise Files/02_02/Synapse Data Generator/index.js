const readline = require("readline");
const fsExtra = require('fs-extra')
var Chance = require('chance');
const { Console } = require("console");
var chance = new Chance();

const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const fileHeaders = [
    { id: 'store', title: 'StoreCode' },
    { id: 'date', title: 'Date' },
    { id: 'sales', title: 'TotalSales' },
    { id: 'amount', title: 'TotalAmount' },
]

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});
var totalRecordsToGenerate = 10000;
var outputFolderName='output';
rl.question("How many records do you want to generate? Increments of One Million : Default(1 Million): ", (recordcount) => {
    rl.close();

    if (recordcount && isNaN(recordcount)) {
        console.log('Not a valid number')
        return;
    }
    if (recordcount && !isNaN(recordcount) && recordcount <= 0) {
        console.log('Enter a number greater then zero')
        return;
    }
    totalRecordsToGenerate = (recordcount || 1) * 1000000;
    cleanOutput().then(()=>{
        main().then(result => {
            console.log('Done')
        });
    }).catch(err=> Console.log('Error cleaning the output folder',err))
   

});

async function main() {
    let records = [];
    let batchSize = 10000;//split records into multiple files.
    let generatedRecordCount = 0;
    let currentBatch = 1;
    let totalBatches = totalRecordsToGenerate / batchSize;
    if (totalBatches == 0) totalBatches = 1;
    let currentStoreIndex=0;
    var stores= fsExtra.readJSONSync('stores.json')
    let totalStores=stores.length;
    console.log(`Total Records To Generate: ${totalRecordsToGenerate}`);
    console.log(`Batch Size: ${batchSize} records per file`);
    console.log(`Writing Batch : ${currentBatch} out of ${totalBatches}`);

    while (generatedRecordCount < totalRecordsToGenerate) {
        let sales=chance.integer({ min: 2000, max: 10000 });
        var date=chance.date();
        records.push({
              store: stores[currentStoreIndex].code,
              date: `${date.getFullYear()}-${(date.getMonth() +1).toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')}`,
              sales: sales,
              amount: sales * chance.integer({ min: 5, max: 100 })
            })
        generatedRecordCount += 1;
        currentStoreIndex+=1;
        if(currentStoreIndex==totalStores){
            currentStoreIndex=1; //reset city back to the first
        }
        if (records.length == batchSize) {
            await writeRecords(records, `sales-${currentBatch}.csv`)
            currentBatch += 1
           if(generatedRecordCount<totalRecordsToGenerate) console.log(`Writing Batch : ${currentBatch} out of ${totalBatches}`);
            records = [];
        }
    }
    if (records.length > 0) {
        await writeRecords(records, `sales-${currentBatch}.csv`)
    }
}

async function writeRecords(records, filename) {
    let csvWriter = createCsvWriter({
        path: `${outputFolderName}/${filename}`,
        header: fileHeaders
    });
    await csvWriter.writeRecords(records);
    console.log(`Created file ${filename} with ${records.length} records`);
}
async function cleanOutput(){
   await fsExtra.emptyDir(outputFolderName)

}

