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
@click.option('--timeout', type=int, default=30, help='最大连接时长，单位为秒')
@click.option('--md5', is_flag=True, help='是否进行md5校验', default=False)
@click.option('--md5_index', type=int, help='md5文件中md5码的位置', default=3)
@click.option('--prefix', type=str, help='前缀筛选')
@click.option('--suffix', type=str, help='后缀筛选')
def download(
    url: str,
    local_path: str,
    *,
    username: str,
    password: str,
    concurrency: int,
    timeout: int,
    md5: bool,
    md5_index: int,
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
            file_list=list(missed_files),
            local_dir=local_path,
            max_concurrency=concurrency,
            timeout=timeout,
        )

    if missed_files:
        asyncio.run(download_func())

    # 校验 MD5
    if md5:
        md5_list = list(download_dir.glob('*.md5'))
        fail_count = 0
        for md5_file in tqdm(md5_list, total=len(md5_list), desc='MD5校验'):
            target_file = download_dir / f'{md5_file.stem}'
            if not verify_md5(target_file, md5_file, md5_index=md5_index):
                target_file.unlink(missing_ok=True)
                md5_file.unlink(missing_ok=True)
                fail_count += 1

        if fail_count > 0:
            logger.error(f'有 {fail_count} 个文件MD5校验失败，已删除。请重新运行此命令以下载')
            exit(0)
        else:
            logger.info('MD5校验通过')


if __name__ == '__main__':
    download()
