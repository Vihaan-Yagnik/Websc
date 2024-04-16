const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs');
const csv = require('csv-parser');
const path = require('path')
// const getUrl = require('./try')
start = 1
end = 50

reqSent = 0;
limit = 3;
reqSentReached = true;
let $;

async function request(url) {
    await sleep(500)

    return await axios.get(url)
}

async function extraApi(url) {
    try {
        return await request(url)
    } catch (error) {
        console.log(`-------------------429,${url}`)
        await writeToCSV(`429,${url}\n`, 'extra-link-log.csv', 'status,link\n')
        await sleep(2500)
        return await extraApi(url)
    }
}

function jsonToCsv(jsonData, companyName, companyId, tableId, isConsolited) {
    // console.log('jsonData',jsonData)
    const data = []
    const expandedabalUrls = []
    for (const [key, value] of Object.entries(jsonData)) {

        for (const [month, val] of Object.entries(value)) {
            if (month == "isExpandable") {
                const [parent, section] = val.replaceAll('Company.showSchedule(', '').replaceAll(')', '').replaceAll("'", '').replaceAll('"', '').split(',')
                expandedabalUrls.push(getUrl('schedules', parent.replaceAll('"', '').trim(), section.trim(), companyId, isConsolited))
                continue;
            }
            try {
                data.push(`${month},${key},${val.replaceAll(',', '').replaceAll('%', '')},${companyName},${tableId}\n`);
            } catch (error) {
                // console.log(val)
                // console.log(error)
            }
        }
    }
    return [data, expandedabalUrls]
}

async function scrapeTable(tableId, companyId, companyName, isConsolited) {
    const data = []
    const table = $(`#${tableId} .responsive-holder table.data-table`);


    const urls = []
    const years = table.find('thead tr th').slice(1).map((index, element) => $(element).text().trim()).get();
    const parameters = table.find('tbody tr').map((index, element) => {
        const td = $(element).find('td')
        const button = $(td).find('button')
        if (button != undefined && tableId != "yearly-shp" && tableId != "quarterly-shp") {
            try {
                const [parent, section] = button.attr('onclick').replaceAll('Company.showSchedule(', '').replaceAll(')', '').replaceAll("'", '').replaceAll('"', '').split(',')
                urls.push(getUrl('schedules', parent.trim(), section.trim(), companyId, isConsolited))
            } catch (error) {
            }
        }
        return td.first().text().trim()
    }
    ).get();
    table.find('tbody tr').each((index, row) => {
        const values = $(row).find('td').slice(1).map((index, element) => $(element).text().trim()).get();
        const parameter = parameters[index];
        years.forEach((year, i) => {
            const value = values[i];
            if (value !== undefined && parameter != "Raw PDF") {
                value2 = value.replaceAll(',', '').replaceAll('%', '')
                data.push(`${year},${parameter},${value2},${companyName},${tableId}\n`);
            }
        });

    });
    for (let i = 0; i < urls.length; i++) {
        let response = await extraApi(urls[i])
        await writeToCSV(`${response.status},${urls[i]}\n`, 'extra-link-log.csv', 'status,link\n')

        const res = jsonToCsv(response.data, companyName, companyId, tableId)
        data.push(...res[0])
        urls.push(...res[1])

    }

    return data.join('');
}

async function scrapeCompanyRatios(comapnyName) {
    const data = [comapnyName]
    const listItems = $('#top-ratios li')
    for (const listItem of listItems) {
        const parameter = $(listItem).find('.name').text().trim()
        const value = $(listItem).find('.value').text().trim()
        data.push(value.replaceAll('\n', '').replaceAll('\t', '').replaceAll(' ', '').replaceAll(',', ''))
    }
    return data.join(',') + '\n'
}

async function scrapePage({ name, code }) {
    try {
        let url = `https://www.screener.in/company/${code}/consolidated/`

        let response = await extraApi(url);
        $ = cheerio.load(response.data);
        let companyRatios = await scrapeCompanyRatios(name);
        if (companyRatios.split(',')[1] === '₹Cr.') {
            url = `https://www.screener.in/company/${code}/`
            let response = await extraApi(url);
            $ = cheerio.load(response.data);
            let companyRatios = await scrapeCompanyRatios(name);
            await writeToCSV(companyRatios, 'comapnyRatios.csv', 'Company Name, Market Cap,Current Price,High / Low,Stock P/E,Book Value,Dividend Yield,ROCE,ROE,Face Value\n')

        } else {

            await writeToCSV(companyRatios, 'comapnyRatios.csv', 'Company Name, Market Cap,Current Price,High / Low,Stock P/E,Book Value,Dividend Yield,ROCE,ROE,Face Value\n')
        }
        let data = [];
        const companyId = $('#company-info').attr('data-company-id')
        const isConsolited = $('#company-info').attr('data-consolidated')
        console.log('isConsolited', isConsolited);
        let tables = ['quarters', 'profit-loss', 'balance-sheet', 'cash-flow', 'ratios', 'quarterly-shp', 'yearly-shp'];
        for (let table of tables) {
            data.push(await scrapeTable(table, companyId, name, isConsolited))
        }
        //company ratio information
        //log
        await writeToCSV(`${name},${code}\n`, 'done.csv', 'Name,BSE Code\n')
        return data;
    } catch (error) {
        console.log(error)
        //log
        await writeToCSV(`${name},${code}\n`, 'error.csv', 'Name,BSE Code\n')
        await sleep(3000)
    }
}

async function writeToCSV(data, fileName, header) {
    try {
        if (!fs.existsSync('csvs')) {
            await fs.mkdirSync(path.join(__dirname, 'csvs'))
        }
        if (!fs.existsSync(path.join(__dirname, 'csvs', fileName))) {
            console.log(fileName)
            await fs.writeFileSync(path.join(__dirname, 'csvs', fileName), header);
        }
        await fs.appendFileSync(path.join(__dirname, 'csvs', fileName), data);
    } catch (error) {
        console.log('Error:', error);
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function main(file) {
    const companies = [];
    fs.createReadStream(file)
        .pipe(csv())
        .on('data', async (row, index) => {

            const code = (row['BSE Code'] !== '') ? row['BSE Code'] : row['NSE Code']
            companies.push({
                name: row.Name,
                code: code,
            })
        })
        .on('end', async () => {
            for (const company of companies) {
                const scrapedData = await scrapePage(company);
                if (scrapedData) {
                    await writeToCSV(scrapedData.join(''), 'output.csv', 'Year,Parameter,Value,Name,Table\n')
                } else {
                    console.log('not scraped properly');
                }
                console.log(`${company.name} is done`)
                // await sleep(1000)
            }
        });
}


function formatUrl(url, data) {
    // Clone data
    var context = {}
    if (data === undefined) data = {}
    Object.keys(data).forEach(function (key) {
        context[key] = data[key]
    })

    // Replace curls
    var parsed = url
    var curls = parsed.match(/{[^}]+}/g)
    if (curls === null) curls = []
    for (var i = 0; i < curls.length; i++) {
        var curlyKey = curls[i]
        var key = curlyKey.replace(/{|}/g, '')
        var value = context[key]
        delete context[key]
        if (value === undefined || value === null) {
            parsed = parsed.replace(curlyKey + '/', '')
        } else {
            parsed = parsed.replace(curlyKey, value)
        }
    }

    // Remaining context goes into get params
    if (Object.keys(context).length > 0) {
        var params = new URLSearchParams(context)
        parsed = parsed + '?' + params.toString()
    }
    return parsed
}
function getUrls() {
    var urls = {
        searchCompany: '/api/company/search/',
        addCompany: '/api/company/{companyId}/add/{listId}/',
        removeCompany: '/api/company/{companyId}/remove/{listId}/',
        quickRatios: '/api/company/{warehouseId}/quick_ratios/',
        peers: '/api/company/{warehouseId}/peers/',
        schedules: '/api/company/{companyId}/schedules/',
        searchRatio: '/api/ratio/search/',
        getChartMetric: '/api/company/{companyId}/chart/',
        searchHsCode: '/api/hs/search/',
        tradeData: '/api/hs/{hsCode}/data/',
        ratioGallery: '/ratios/gallery/',
        getSegments: '/api/segments/{companyId}/{section}/{segtype}/',
        addPushSubscription: '/api/notifications/add_push_subscription/',
        companyInSublists: '/api/company/sublists/{companyId}/',
        getShareholders: '/api/3/{companyId}/investors/{classification}/{period}/',
        filterAnnualReports: '/annual-reports/filter/',
        notes: '/notebook/{companyId}/',
        notesUpload: '/notebook/upload/'
    }

    //https://www.screener.in/api/segments/11/profit-loss/full/
    return urls
}

function getUrl(name, parent, section, companyId, isConsolited) {
    data = {
        parent: parent,
        companyId: companyId,
        section: section,
    }
    if (isConsolited) {

        data.consolidated = ''
    }

    // console.log(data)
    var urls = getUrls()
    var url = urls[name]
    return formatUrl('https://www.screener.in' + url, data)
}
const file = path.join(__dirname, 'batches', 'batch-1.csv')
const index = 1
main(file);
// main('done.csv')
if (index == 49) {
    writeToCSV(file + '\n', 'batch_run_already.csv', 'file name\n')
}