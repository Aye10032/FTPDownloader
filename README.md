# Simple FTP download

## Install

建议使用`UV`进行环境配置，详情参考：[install uv](https://docs.astral.sh/uv/getting-started/installation/)

```bash
git clone https://github.com/Aye10032/FTPDownloader.git
cd FTPDownloader
uv venv
# 根据提示激活环境
uv sync
```

## Usage

示例1：从NCBI下载nr数据库

```bash
python main.py --concurrency 5 --timeout 3600 --prefix nr --md5 https://ftp.ncbi.nlm.nih.gov/blast/db/ /your_db_path/nr
```

由于单个文件较大，设置`timeout`为3600秒（一小时）