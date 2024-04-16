const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs');
const csv = require('csv-parser');

start = 1
end = 50
async function scrapeProfitLoss({name, code}) {
    try {
        const url = `https://www.screener.in/company/${code}/consolidated/`
        const response = await axios.get(url);
        const $ = cheerio.load(response.data);
        let data = [];
        let tables = ['profit-loss', 'balance-sheet', 'cash-flow', 'ratios', 'shareholding'];
        for (let table of tables) {
            const profitLossTable = $(`#${table} .responsive-holder`);

            const years = profitLossTable.find('thead tr th').slice(1).map((index, element) => $(element).text().trim()).get();
            const parameters = profitLossTable.find('tbody tr').map((index, element) => $(element).find('td').first().text().trim()).get();

            profitLossTable.find('tbody tr').each((index, row) => {
                const values = $(row).find('td').slice(1).map((index, element) => $(element).text().trim()).get();
                const parameter = parameters[index];
                years.forEach((year, i) => {
                    const value = values[i];
                    if (value !== undefined) {
                        value2 = value.replaceAll(',','').replaceAll('%','')
                        data.push({ year, parameter, value: value2 , name});
                    }
                });
            });
        }
        await writeToCSVlog({name,code},'done.csv')

        return data;
    } catch (error) {
        // console.log(error)
        await writeToCSVlog({name,code},'error.csv')
    }
}

async function writeToCSV(data) {
    try {
        const csvRows = [];
        data.forEach(entry => {
            csvRows.push(`${entry.year},${entry.parameter},${entry.value},${entry.name}\n`);
        });
        fs.appendFileSync('test-output.csv', csvRows.join(''));
    } catch (error) {
        console.error('Error:', error);
    }
}

async function writeToCSVlog({name,code},logFile) {
    try {
        fs.appendFileSync(logFile, `${name},${code}\n`);
    } catch (error) {
        console.error('Error:', error);
    }
}
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
async function main(file) {
    const companies = [];
    console.log('main started');
    //aya jenish tane taari file nakhvani che test na jagya e jenish 
    fs.createReadStream(file)
        .pipe(csv())
        .on('data', async (row, index) => {
         
            const code = (row['BSE Code'] !== '') ? row['BSE Code'] : row['NSE Code']
            companies.push({
                name:row.Name,
                code : code,
            })
            // const profitLossData = await scrapeProfitLoss(code);
        })
        .on('end', async () => {
            for (const company of companies) {
                const profitLossData = await scrapeProfitLoss(company);
                if (profitLossData) {
                    await writeToCSV(profitLossData)
                    
                }
            }
        });

}

main('retry.csv');
