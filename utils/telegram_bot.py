import asyncio
import json

import pendulum
from airflow.models import Variable
from telegram.ext import Application


class Bot:
    def __init__(self):
        self.token = Variable.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = Variable.get("CHAT_ID")
        self.ids = json.load(open(Variable.get("IDS_PATH"), "r"))
        self.app = Application.builder().token(self.token).build()

    async def send_telegram_message(self, dag, owner):
        message = f"""
❗️ *Alert* ❗️

*Dag:* `{dag.dag_display_name}`
*Status:* `Failed`
*Time:* `{pendulum.now("Asia/Tehran").strftime("%Y-%m-%d %H:%M:%S")}`
{owner}
Please check the dag for any issues
        """
        await self.app.bot.send_message(
            chat_id=self.chat_id, text=message, parse_mode="Markdown"
        )

    def send_message(self, dag):
        asyncio.run(self.send_telegram_message(dag, self.owners(dag)))

    def owners(self, dag):
        owners = [self.ids.get(owner, None) for owner in dag.owner.split(", ")]
        if owners.count(None) == len(owners):
            return ""
        else:
            msg = (
                "*Owners:*\n"
                + "\n".join(
                    [
                        f"🤓 [{owner}](tg://user?id={self.ids.get(owner)})"
                        for owner in dag.owner.split(", ")
                        if self.ids.get(owner, None)
                    ],
                )
                + "\n"
            )
            return msg
