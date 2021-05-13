from configuration import logger, max_attempts


class DownloadError(Exception):
    pass


def tm1_reader(tm1, mdx, name):
    logger.info(f'starting download of {name}')
    result = try_many(tm1, mdx, name)
    if result:
        logger.info(f'download of {name} completed - {len(result[1]["Cells"])} cells retrieved')
    else:
        logger.info(f'download of {name} failed - max number of attempts reached')
    return result


def try_many(tm1, mdx, name):
    for i in range(1, max_attempts + 1):
        if i > 1:
            logger.info(f'retrying {name} - try {i} of {max_attempts}')
        try:
            return i,download(tm1, mdx, name)
        except DownloadError as e:
            pass
    return None


def download(tm1, mdx, name):
    try:
        cellset_id = tm1.cells.create_cellset(mdx)
        return tm1.cells.extract_cellset_raw(cellset_id)
    except Exception as e:
        logger.warning(f'download error while downloading {name} - error: {e}')
        raise DownloadError(e)
