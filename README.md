# Road Closure Bot 🚧

A Python bot that monitors and tweets about road closures in Cameron County, TX, particularly those related to SpaceX activities.

## Features

- 🔍 Automatically scrapes the Cameron County website for road closure notices
- 🔄 Tracks closure status (active, revoked, expired)
- 🐦 Posts notifications to Twitter when closures are announced or changed
- 💾 Stores closure data in a local SQLite database
- 🔔 Handles both regular road closures and SpaceX flight-related closures

## Requirements

- Python 3.7+
- Required packages listed in `requirements.txt`
- Twitter API credentials

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/road-closure-bot.git
   cd road-closure-bot
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create a `.env` file with your Twitter API credentials:
   ```
   TWITTER_CONSUMER_KEY=your_consumer_key
   TWITTER_CONSUMER_SECRET=your_consumer_secret
   TWITTER_ACCESS_TOKEN=your_access_token
   TWITTER_ACCESS_TOKEN_SECRET=your_access_token_secret
   TWITTER_BEARER_TOKEN=your_bearer_token
   CHECK_INTERVAL=60
   ```

## Usage

Run the bot with:

```
python road_closure_bot.py
```

The bot will continuously monitor for road closures based on the check interval specified in the `.env` file.

## Logging

The bot logs activity to `road_closure_bot.log` and also outputs information to the console.

## Database

Closure data is stored in a SQLite database (`road_closures.db`).

## License

[MIT License](LICENSE)

## Acknowledgements

- [Cameron County, TX](https://www.cameroncountytx.gov/spacex/) for providing road closure information
- [Tweepy](https://www.tweepy.org/) for Twitter API integration
- [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/) for web scraping

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 