from log import logger
import jobs
import schedule
import time


if __name__ == '__main__':
    logger.info("Bi sync service started!")

    all_jobs = schedule.get_jobs()

    logger.info(all_jobs)

    while True:
        schedule.run_pending()
        time.sleep(1)
