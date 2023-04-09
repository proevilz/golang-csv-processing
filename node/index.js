
const fs = require('fs');
const { stringify, parse, transform } = require('csv');

const { v4: uuidv4 } = require('uuid');
const { faker } = require('@faker-js/faker');

const main = async () => {
    const start = performance.now();
    const transformFunction = (record) => {
        record.id = uuidv4();
        record.email = faker.internet.email();
        return record
    }


    const csvFileName = 'random_data.csv';
    const inputStream = fs.createReadStream(csvFileName);
    const outputStream = fs.createWriteStream('output.csv');
    const parser = parse({ columns: true, delimiter: ',', skip_empty_lines: true })

    const transformer = transform(transformFunction)
    const stringifier = stringify({ header: true });

    inputStream.pipe(parser).pipe(transformer).pipe(stringifier).pipe(outputStream);
    outputStream.on('finish', () => {
        const end = performance.now();
        const elapsedTimeInMilliseconds = end - start;
        const elapsedTimeInSeconds = elapsedTimeInMilliseconds / 1000;

        console.log(`myFunction took ${elapsedTimeInSeconds} seconds to execute.`);
    });

}

main()