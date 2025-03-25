# Simple FTP download
![Python Version from PEP 621 TOML](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2FAye10032%2FFTPDownloader%2Frefs%2Fheads%2Fmaster%2Fpyproject.toml)


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