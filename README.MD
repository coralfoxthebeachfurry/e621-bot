# e621 Discord Bot

A Discord bot that fetches and posts images from e621.net. Supports scheduled posts and manual search commands with automatic duplicate prevention and video filtering.

## Features

- **Scheduled posting** – Automatically posts to a specified NSFW channel at regular intervals.
- **Manual search** – Use `!e621 [tags]` to fetch random posts from e621.
- **No duplicates** – Tracks posted images in a SQLite database to avoid repeats.
- **Video filtering** – Excludes MP4 and WebM videos (Discord embeds don’t support them). GIFs are kept.
- **Rate‑limit handling** – Retries with exponential backoff on 429 responses.
- **Random sorting** – Adds `order:random` when no sort order is specified by the user.
- **Artist info & descriptions** – Shows artist names and post descriptions in embeds.

## Setup

### Prerequisites

- Python 3.8 or higher
- A Discord bot token ([Discord Developer Portal](https://discord.com/developers/applications))
- (Optional) e621 API credentials – you can run without them, but they allow higher rate limits.

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/coralfoxthebeachfurry/e621-discord-bot.git
   cd e621-discord-bot 

2. Install dependencies:
   ```bash
   pip install -r requirements.txt

3. Copy .env.example to .env and fill in the values

4. Run the bot!
   ```bash
   python main.py
   
## Commands

- **!e621 [tags]** - search for a random post with given tags. If no tags were given,
uses the default TAGS from **.env**.

Example:

**!e621 wolf rating:s**

**Note:** The command is restricted to NSFW only channels.

## Database

The bot uses SQLite to store already posted images IDs to prevent the same
post being shown twice. You can delete **posted.db** to reset it's history.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Acknowledgements

- **e621.net** for the API
- **discord.py** library
- **All contributors and testers**

**NOTE:** This bot should only be used in NSFW channels due to Discord guidelines
and E621 content policies.
