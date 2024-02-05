# Gap Filler

This is a simple tool to fill gaps in a sequence of timeseries candle data. It is useful when you have a sequence of data with gaps and you want to fill them with the correct data from a data provider.

## More Information

### Timeframes

 It currently supports filling gaps in the following timeframes: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1M; however, there is still lots of code improvement required to change timeframes easily.

### Data Provider

It currently only supports Binance data, but it can be easily extended to support other data providers by inspecting their API documentation and writing a custom converter.

### Gaps

The program will find the gaps by fetching all available data for the given symbol and timeframe. Then it searches for a specified timestamps, and creates a list of gaps which contains `startTime`s and `endTime`s of the gaps. After that, it fills gaps in the data by querying the data provider for the missing data. It will then insert the missing data into the database.

### Database

The program uses PostgreSQL as the database. It uses the `sqlx` crate to interact with the database. The database connection is established using the `dotenv` crate, which reads the environment variables from the `.env` file.

### Asynchronous

The program is asynchronous and uses the `tokio` runtime to run asynchronous tasks. It uses the `binance-rs-async` crate to fetch data from the Binance API. It also uses Semaphore to limit and control the number of concurrent gap checking and filling tasks.

### Time

The program uses the `chrono` crate to handle time and date.

## Usage

Install Rust and Cargo from [here](https://www.rust-lang.org/tools/install).

Clone the repository and navigate to the root directory of the project.

Copy the `.env.template` file to `.env` and fill in the required environment variables.

Check your tables in the database and make sure that the table you want to fill gaps in has the correct columns. The columns should be named `timestamp`, `open`, `high`, `low`, `close`, `volume`, `tickersymbol` and `interval`. If you have different column names, you can change them in the `fill_gaps` function in `src/main.rs`.

Run the following command to fill gaps in the data:

```bash
cargo run
```

or if you want to run the program in release mode:

```bash
cargo run --release
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
