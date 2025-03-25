import asyncio
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import click
from loguru import logger
from tqdm.asyncio import tqdm

from utils.ftp import FTPClient, download_http_files
from utils.md5 import verify_md5


@click.command()
@click.argument('url', type=str, required=True)
@click.argument('local_path', type=str, required=True)
@click.option('--username', '-u', type=str, default='anonymous')
@click.option('--password', '-p', type=str, default='')
@click.option('--concurrency', type=int, default=3, help='最大并发下载数')
@click.option('--md5', is_flag=True, help='是否进行md5校验', default=False)
@click.option('--prefix', type=str, help='前缀筛选')
@click.option('--suffix', type=str, help='后缀筛选')
def download(
    url: str,
    local_path: str,
    *,
    username: str,
    password: str,
    concurrency: int,
    md5: bool,
    prefix: Optional[str] = None,
    suffix: Optional[str] = None,
):
    if not url.startswith(('http://', 'https://', 'ftp://')):
        logger.error('必须是一个标准http/ftp url')
        exit(0)

    parsed = urlparse(str(url))
    host, remote_path = parsed.netloc, parsed.path

    with FTPClient(host=host, username=username, password=password) as ftp:
        files = ftp.list_files(remote_path)

    exist_files = []
    download_dir = Path(local_path)
    download_dir.mkdir(parents=True, exist_ok=True)

    for file_path in download_dir.iterdir():
        if file_path.is_file():
            if file_path.stat().st_size == 0:
                file_path.unlink(missing_ok=True)
            else:
                exist_files.append(file_path.name)

    missed_files = set(files) - set(exist_files)

    if prefix:
        missed_files = {file for file in missed_files if file.startswith(prefix)}

    if suffix:
        missed_files = {file for file in missed_files if file.endswith(suffix)}

    async def download_func():
        await download_http_files(
            base_url=f'https://{host}',
            remote_path=remote_path,
            file_list=list(missed_files)[:10],
            local_dir=local_path,
            max_concurrency=concurrency,
        )

    if missed_files:
        asyncio.run(download_func())

    # 校验 MD5
    if md5:
        gz_list = list(download_dir.glob('*.gz'))
        fail_count = 0
        for gz_file in tqdm(gz_list, total=len(gz_list), desc='MD5校验'):
            md5_file = download_dir / f'{gz_file.name}.md5'
            if not verify_md5(gz_file, md5_file):
                gz_file.unlink(missing_ok=True)
                md5_file.unlink(missing_ok=True)
                fail_count += 1

        if fail_count > 0:
            logger.error(f'有 {fail_count} 个文件MD5校验失败，已删除。请重新运行此命令以下载')
            exit(0)
        else:
            logger.info('MD5校验通过')


if __name__ == '__main__':
    download()
