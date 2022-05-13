from jobs.Get_Zoho_CRM_Modules import main as get_zoho_modules
from  jobs.Get_Zoho_Books_Modules import main as get_zoho_books_modules
import time
from schedule import every, repeat, run_pending
from log import logger


@repeat(every(15).minutes)
def zoho_sync():
    start = time.time()
    logger.info('Starting Run_main')
    get_zoho_modules()
    get_zoho_books_modules()
    end = time.time()
    execution_time = end - start
    execution_time = round(execution_time, 1)
    logger.info('The Execution Time was {} seconds'.format(execution_time))
