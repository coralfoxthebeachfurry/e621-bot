from dotenv import load_dotenv
import sqlite3
import discord
from discord.ext import commands, tasks
import random
import os
import logging
import aiohttp
import asyncio
from contextlib import contextmanager

#variables etc
load_dotenv()
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID', '0')
COMMAND_PREFIX = os.getenv('COMMAND_PREFIX')
E621_API_KEY = os.getenv('E621_API_KEY')
E621_USERNAME = os.getenv('E621_USERNAME')
TAGS = os.getenv('TAGS', 'order:hot')
POST_INTERVAL = int(os.getenv('POST_INTERVAL', '60'))
USER_AGENT = 'E621-Discord-Bot/1.0 (by @coralfoxthebeachfurry)'

#logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#database
@contextmanager
def get_db():
    conn = sqlite3.connect('posted.db')
    try:
        yield conn
    finally:
        conn.close()

#initialize the DB
def init_db():
    with get_db() as conn:
        conn.execute('''
                      CREATE TABLE IF NOT EXISTS posted_posts (
                        post_id INTEGER PRIMARY KEY,
                        posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                      )
                    ''')
        conn.commit()

#check if post has been used
def is_post_used(post_id):
    with get_db() as conn:
        cursor = conn.execute('SELECT 1 FROM posted_posts WHERE post_id = ?', (post_id,))
        return cursor.fetchone() is not None

#marks the post as used
def mark_post_used(post_id):
    with get_db() as conn:
        conn.execute ('INSERT OR IGNORE INTO posted_posts (post_id) VALUES (?)', (post_id,))
        conn.commit()

#bot setup 
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents)

class E621Bot(commands.Cog):
    #initialize
    def __init__(self, bot):
        self.bot = bot
        self.session = None
        self.base_url = "https://e621.net"
        self.headers = {"User-Agent": USER_AGENT}
    #gets the session
    async def get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(headers=self.headers)
        return self.session
    #fetch posts
    async def fetch_posts(self, tags, limit=50, page=0):
        session = await self.get_session()
        url = (f"{self.base_url}/posts.json")
        params = {"tags": tags.replace(' ', '+'), "limit": limit, "page": page}
        auth = None
        if E621_USERNAME and E621_API_KEY:
            auth = aiohttp.BasicAuth(E621_USERNAME, E621_API_KEY)

        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with session.get(url, params=params, auth=auth) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get('posts', [])
                    elif resp.status == 429:
                        wait = 5 * (attempt + 1)
                        logger.warning(f"Rate limited, waiting {wait} seconds...")
                        await asyncio.sleep(wait)
                    else:
                        logger.error(f"API error: {resp.status}")
                        return []
                
            except Exception as e:
                logger.error(f'Fetch error: {e}')
                return[]

        logger.error("Max retries exceeded due to rate limiting!")
        return []

    #checks if a post have valid image/gif url
    def has_valid_image(self, post):
        #checks if the post has valid image
        image_url = post.get('file', {}).get('url')
        return image_url is not None and image_url != ""

    #get unposted posts
    def get_unposted_post(self, posts):
        for post in posts:
            if not is_post_used(post['id']) and self.has_valid_image(post):
                return post
        return None

    #sends the post
    async def send_post(self, channel, post):
        #extract data(artist, desc, tag)
        tags = post.get('tags', {})
        artists = tags.get('artist', [])
        artist_text = artists[0] if artists else "Unknown"
        if len(artists) > 1:
            artist_text += f" and {len(artists)-1} others"

        desc = post.get('description', '').strip()
        if not desc:
            desc = "*No description*"
        elif len(desc) > 500:
            desc = desc[:500] + "..."

        #create embed
        embed = discord.Embed(
            title=f"Artist: {artist_text}",
            description=desc,
            url=f"https://e621.net/posts/{post['id']}",
            color=discord.Color.purple()
        )

        #image 
        image_url = post.get('file', {}).get('url')
        if not image_url:
            logger.warning(f"Skipping post {post['id']}. No image URL")
            return False
        embed.set_image(url=image_url)

        #send the embed
        await channel.send(embed=embed)
        mark_post_used(post['id'])
        return True

    @tasks.loop(minutes=POST_INTERVAL)
    #scheduled task
    async def scheduled_post(self):
        try:
            channel = self.bot.get_channel(int(CHANNEL_ID))
        except ValueError:
            logger.error("Error! CHANNEL_ID is not a valid int.")
            return

        if not channel or not channel.is_nsfw():
            logger.error("Channel not found or not NSFW")
            return
        
        base_tags = TAGS
        #handles missing tags for better support
        if 'order:' not in base_tags:
            base_tags += ' order:random'
        if '-mp4' not in base_tags:
            base_tags += ' -mp4'
        if '-webm' not in base_tags:
            base_tags += ' -webm'

        posts = await self.fetch_posts(base_tags, limit=100)
        if not posts:
            return
    
        post = self.get_unposted_post(posts)
        if post:
            await self.send_post(channel, post)
        else:
            logger.info("No new posts found in this batch. Trying random page...")
            random_page = random.randint(1, 10)
            posts = await self.fetch_posts(base_tags, limit=100, page=random_page)
            if posts:
                post = self.get_unposted_post(posts)
                if post:
                    await self.send_post(channel, post)
                    return
            logger.info("No new posts found after multiple attempts.")

    @scheduled_post.before_loop
    async def before_scheduled(self):
        await self.bot.wait_until_ready()

    @commands.command(name="e621")
    @commands.is_nsfw()
    async def e621_command(self, ctx, *, tags=None):
        search_tags = tags if tags else TAGS
        #handles missing tags for better support
        if 'order:' not in search_tags:
            search_tags += ' order:random'
        if '-mp4' not in search_tags:
            search_tags += ' -mp4'
        if '-webm' not in search_tags:
            search_tags += ' -webm'

        async with ctx.typing():
            found_post = None
            for page in range(5):
                posts = await self.fetch_posts(search_tags, limit=50, page=page)
                if not posts:
                    continue
                random.shuffle(posts)
                found_post = self.get_unposted_post(posts)
                if found_post:
                    break

            if not found_post:
                #as a fallback if cant find anymore posts
                posts = await self.fetch_posts(search_tags, limit=50)
                valid_posts = [p for p in posts if self.has_valid_image(p)]
                if valid_posts:
                    found_post = random.choice(valid_posts)
                    await ctx.send("No new posts found. Showing a previously posted image.")
                else:
                    await ctx.send("No posts found with these tags.")
                    return

            await self.send_post(ctx.channel, found_post)

    @e621_command.error
    async def e621_error(self, ctx, error):
        if isinstance(error, commands.NSFWChannelRequired):
            await ctx.send("NSFW channel required")

@bot.event
async def on_ready():
    logger.info(f"{bot.user} is ready")
    init_db()
    await bot.add_cog(E621Bot(bot))

#runs the bot
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.error("No token found.")
        exit(1)
    bot.run(DISCORD_TOKEN)
