const fs = require('fs');
const csv = require('csv-parser');
const path = require('path')

mainFile = 'companies.csv'
if (!fs.existsSync('batches')) {
    fs.mkdirSync(path.join(__dirname, 'batches'))
}
function jsonToCsv(jsonData, companyName, companyId) {
    const data = []

    for (const [month, val] of Object.entries(jsonData)) {
        data.push(val)
    }

    return data.join(',')
}
async function createBatches(file, header, limit) {
    rows = []
    fs.createReadStream(file)
        .pipe(csv())
        .on('data', async (row, index) => {
            rows.push(jsonToCsv(row) + '\n')
        })
        .on('end', async () => {
            batchCount = 1
            while (rows.length != 0) {
                await writeToCSV(rows.splice(0, 50).join(''), `batches/batch-${batchCount}.csv`, header)
                batchCount += 1
            }
        });
}

async function writeToCSV(data, fileName, header) {
    try {
        if (!fs.existsSync(fileName)) {
            await fs.writeFileSync(fileName, header);
        }
        await fs.appendFileSync(fileName, data);
    } catch (error) {
        console.error('Error:', error);
    }
}

createBatches(mainFile, 'Name,BSE Code,NSE Code,Industry,Current Price,Return over 1day,Market Capitalization,Sales,Profit after tax,Debt,Price to Earning,PEG Ratio,Dividend yield,Dividend Payout,Return on equity,Return on capital employed,EVtoSales,EVEBITDA,Sales growth,Sales growth 3Years,Sales growth 5Years,Profit growth,Profit growth 3Years,Profit growth 5Years,Promoter holding,Enterprise Value,Return over 6months,Price to book value,Return over 1year,Return over 3years,Return over 5years,Average return on equity 3Years,Average return on equity 5Years,Return on assets,Historical PE 3Years,Historical PE 5Years,DMA 50,DMA 200\n', 100)