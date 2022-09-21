import logging
import os
import sys

from markdown import Markdown
from nio import AsyncClient
from nio.responses import ErrorResponse, RoomResolveAliasError

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

# Use relative path for custom modules (easier to handle with airflow deployment atm)
sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, '../../../ckanqa'))))
from ckanqa.constant import MATRIX_ROOM_ID_ALL


class MatrixHook(BaseHook):

    def __init__(self, conn_id: str):
        super().__init__()
        self.conn = self.get_connection(conn_id)
        if isinstance(self.conn.host, str) and isinstance(self.conn.login, str):
            pass
        else:
            raise ValueError('Stored host and login must be set.')
        self.client = AsyncClient(self.conn.host, self.conn.login)

    async def _login(self):
        """Returns token..."""
        r = await self.client.login(self.conn.password)
        logging.info('Logged in to matrix homeserver.')
        return r


    async def _close(self):
        r = await self.client.close()
        logging.info('Logged out of matrix homeserver')
        return r

    async def resolve_alias(self, alias: str):
        await self._login()
        try:
            r = await self.client.room_resolve_alias(alias)
            if isinstance(r, RoomResolveAliasError):  # When room does not exist, create one
                raise AirflowException(f'Room alias {alias} could not be resolved.')
        finally:
            await self._close()
        return r.room_id

    async def send_markdown_message(self, room_id: str, message: str):
        """Sends message to matrix server.

        This method needs error handling, e.g. when sent message is too big.

        """
        login_r = await self._login()
        try:
            msg_html = Markdown().convert('<pre><code>' + message + '</code></pre>')
            send_r = await self.client.room_send(
                room_id=room_id,
                message_type='m.room.message',
                content={
                    "msgtype": "m.text",
                    "format": "org.matrix.custom.html",
                    "body": message,
                    "formatted_body": msg_html,
                }
            )
        finally:
            close_r = await self._close()
