const express = require('express');
const app = express();
const fs = require('fs').promises;
const csv = require('csv-parser');
const streamifier = require('streamifier');
const moment = require('moment');
const cors = require('cors');
require('dotenv').config();

const port = process.env.PORT || 4000;

// Enable CORS for all routes
app.use(cors());


const readCSV = async () => {
    try {
        const results = [];
        const fileContent = await fs.readFile('dataset.csv', 'utf-8');
        const fileStream = streamifier.createReadStream(fileContent);

        await new Promise((resolve, reject) => {
            fileStream
                .pipe(csv())
                .on('data', (data) => results.push(data))
                .on('end', () => {
                    resolve();
                })
                .on('error', (error) => {
                    reject(error);
                });
        });

        return results;
    } catch (error) {
        throw error;
    }
};

const calculateHourlyAverage = (data) => {
    const hourlyAverages = {};

    data.forEach((item) => {
        const timestamp = moment(item.Timestamp);
        const roundedTimestamp = timestamp.startOf('hour').format('YYYY-MM-DD HH:mm:ss');

        if (!hourlyAverages[roundedTimestamp]) {
            hourlyAverages[roundedTimestamp] = {
                Timestamp: roundedTimestamp,
                'Average Profit Percentage': parseFloat(item['Profit Percentage']),
                Count: 1,
            };
        } else {
            hourlyAverages[roundedTimestamp]['Average Profit Percentage'] +=
                parseFloat(item['Profit Percentage']);
            hourlyAverages[roundedTimestamp]['Count'] += 1;
        }
    });

    // Calculate the final average for each hour
    Object.keys(hourlyAverages).forEach((timestamp) => {
        hourlyAverages[timestamp]['Average Profit Percentage'] /=
            hourlyAverages[timestamp]['Count'];
        // If you want to round the average to a specific number of decimal places:
        hourlyAverages[timestamp]['Average Profit Percentage'] = parseFloat(
            hourlyAverages[timestamp]['Average Profit Percentage'].toFixed(2)
        );
    });

    return Object.values(hourlyAverages);
};
const calculateYearlyAverage = (data) => {
    const yearlyAverages = {};

    data.forEach((item) => {
        const timestamp = moment(item.Timestamp);
        const year = timestamp.year();
        // console.log(timestamp);
        // console.log(year);

        if (!yearlyAverages[year]) {
            yearlyAverages[year] = {
                Year: year,
                'Total Profit Percentage': parseFloat(item['Average Profit Percentage']),
                Count: 1,
            };
        } else {
            yearlyAverages[year]['Total Profit Percentage'] +=
                parseFloat(item['Average Profit Percentage']);
            yearlyAverages[year]['Count'] += 1;
        }
        // console.log(item['Average Profit Percentage'], yearlyAverages[year]);
    });


    // Calculate the final average for each year
    Object.keys(yearlyAverages).forEach((year) => {
        if (yearlyAverages[year]['Count'] > 0) {
            yearlyAverages[year]['Average Profit Percentage'] =
                yearlyAverages[year]['Total Profit Percentage'] /
                yearlyAverages[year]['Count'];
            yearlyAverages[year]['Average Profit Percentage'] = parseFloat(
                yearlyAverages[year]['Average Profit Percentage'].toFixed(2)
            );
        } else {
            console.log("Year ${ year } has no valid data.");
        }
    });

    return Object.values(yearlyAverages);
};

const convertToArrayYear = (data) => {
    let year = [];
    let yearlyProfit = [];
    data.forEach((item) => {
        year.push(item.Year);
        yearlyProfit.push(item['Average Profit Percentage']);
    });
    return [year, yearlyProfit];
}






const calculateMonthlyAverage = (data) => {
    const monthlyAverages = {};
    // const quaters = [
    //     'Jan - '
    // ]
    const months = [
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'June', 'July', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
    ];


    data.forEach((item) => {
        const timestamp = moment(item.Timestamp);
        const monthIdx = timestamp.month();
        const year = timestamp.year();

        // console.log(timestamp);
        // console.log(monthIdx);

        let month = months[monthIdx] + ' ' + year;

        if (!monthlyAverages[month]) {
            monthlyAverages[month] = {
                Month: month,
                'Total Profit Percentage': parseFloat(item['Average Profit Percentage']),
                Count: 1,
            };
        } else {
            monthlyAverages[month]['Total Profit Percentage'] +=
                parseFloat(item['Average Profit Percentage']);
            monthlyAverages[month]['Count'] += 1;
        }
        // console.log(item['Average Profit Percentage'], monthlyAverages[month]);
    });


    // Calculate the final average for each month
    Object.keys(monthlyAverages).forEach((month) => {
        if (monthlyAverages[month]['Count'] > 0) {
            monthlyAverages[month]['Average Profit Percentage'] =
                monthlyAverages[month]['Total Profit Percentage'] /
                monthlyAverages[month]['Count'];
            monthlyAverages[month]['Average Profit Percentage'] = parseFloat(
                monthlyAverages[month]['Average Profit Percentage'].toFixed(2)
            );
        } else {
            console.log("Month ${ month } has no valid data.");
        }
    });

    return Object.values(monthlyAverages);
};

const convertToArrayMonth = (data) => {
    let month = [];
    let monthlyProfit = [];
    data.forEach((item) => {
        month.push(item.Month);
        monthlyProfit.push(item['Average Profit Percentage']);
    });
    return [month, monthlyProfit];
}


const calculatequaterlyAverage = (data) => {
    const quaterlyAverages = {};

    const quaters = [
        'Jan-Mar', 'Apr-June', 'July-Sep', 'Oct-Dec'
    ];


    data.forEach((item) => {
        const timestamp = moment(item.Timestamp);
        const monthIdx = timestamp.month();
        const year = timestamp.year();

        // console.log(timestamp);
        // console.log(monthIdx);

        let quater;
        if (monthIdx < 3) {
            quater = quaters[0] + ' ' + year;
        } else if (monthIdx < 6) {
            quater = quaters[1] + ' ' + year;
        } else if (monthIdx < 9) {
            quater = quaters[2] + ' ' + year;
        } else if (monthIdx < 12) {
            quater = quaters[3] + ' ' + year;
        }


        if (!quaterlyAverages[quater]) {
            quaterlyAverages[quater] = {
                quater: quater,
                'Total Profit Percentage': parseFloat(item['Average Profit Percentage']),
                Count: 1,
            };
        } else {
            quaterlyAverages[quater]['Total Profit Percentage'] +=
                parseFloat(item['Average Profit Percentage']);
            quaterlyAverages[quater]['Count'] += 1;
        }
        // console.log(item['Average Profit Percentage'], monthlyAverages[month]);
    });


    // Calculate the final average for each month
    Object.keys(quaterlyAverages).forEach((quater) => {
        if (quaterlyAverages[quater]['Count'] > 0) {
            quaterlyAverages[quater]['Average Profit Percentage'] =
                quaterlyAverages[quater]['Total Profit Percentage'] /
                quaterlyAverages[quater]['Count'];
            quaterlyAverages[quater]['Average Profit Percentage'] = parseFloat(
                quaterlyAverages[quater]['Average Profit Percentage'].toFixed(2)
            );
        } else {
            console.log("quater ${ month } has no valid data.");
        }
    });

    return Object.values(quaterlyAverages);
};

const convertToArrayquater = (data) => {
    let quater = [];
    let quaterlyProfit = [];
    data.forEach((item) => {
        quater.push(item.quater);
        quaterlyProfit.push(item['Average Profit Percentage']);
    });
    return [quater, quaterlyProfit];
}



app.get('/', async (req, res) => {
    try {
        const result = await readCSV();
        const downsampl = calculateHourlyAverage(result);
        const quaterData = calculatequaterlyAverage(downsampl);
        const quaterArray = convertToArrayquater(quaterData);

        const monthData = calculateMonthlyAverage(downsampl);
        const monthArray = convertToArrayMonth(monthData);

        const yearData = calculateYearlyAverage(downsampl);
        const yearArray = convertToArrayYear(yearData);

        const dataObject = {
            months: monthArray,
            quarters: quaterArray,
            years: yearArray
          };

        res.json(dataObject);

    } catch (error) {
        console.error(error);
        res.status(500).send('Internal Server Error');
    }
});

app.listen(port, () => {
    console.log(`Server is listening on port ${port}`);
  });