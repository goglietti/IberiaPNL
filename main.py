import json
import threading
import concurrent.futures
import time
from concurrent.futures import ThreadPoolExecutor
from TM1py import TM1Service
from templates import mdx_template, create_stmt
import pandas as pd
from configuration import logger, tm1_connection, max_workers, app_name, target
from readers import tm1_reader
from writers import writer
from initializer import load_supporting_attributes_and_cubes


def worker(tm1, customer):

    thread_id = threading.get_ident()
    mdx = mdx_template.format(customer)
    start = time.time()
    cell_count = None
    status = 'FAILED'
    result = tm1_reader(tm1, mdx, customer)
    if result:
        tries,data = result
        writer(data, customer)
        logger.info(f'customer {customer} saved')
        status = 'SUCCESS'
        cell_count = len(data['Cells'])
    started_at = time.strftime("%b %d %Y %H:%M:%S", time.gmtime(start))
    end = time.time()
    ended_at = time.strftime("%b %d %Y %H:%M:%S", time.gmtime(end))
    duration = end - start
    worker_info = {'customer': customer, 'count': cell_count, 'started_at': started_at, 'ended_at': ended_at,
                   'duration': duration, 'status': status, 'thread_id': thread_id, 'tries': tries}
    return worker_info


def main():

    logger.info(f"{'=' * 100}")
    logger.info(f"Application {app_name} started at {current_time}")
    logger.info(f"max_workers: {max_workers}")
    logger.info(f'saving to {target}')
    logger.info(f'associated performance file {performance_file_name}.csv')


    with TM1Service(**tm1_connection) as tm1:
        from_largest_to_smaller = ['PC_035', 'PC_109', 'PC_065', 'PC_108', 'PC_107', 'PC_024', 'PC_001', 'PC_013',
                                   'PC_025', 'PC_034', 'PC_023', 'PC_093', 'PC_102', 'PC_086', 'PC_004', 'PC_002',
                                   'PC_005', 'PC_008', 'PC_106', 'PC_105', 'PC_061', 'PC_094', 'PC_103', 'PC_003',
                                   'PC_111', 'PC_030', 'PC_039', 'PC_063', 'PC_064', 'PC_036', 'PC_066', 'PC_110',
                                   'PC_027', 'PC_048', 'PC_058', 'PC_115', 'PC_067', 'PC_068', 'PC_032', 'PC_047',
                                   'PC_016', 'PC_042', 'PC_070', 'PC_053', 'PC_052', 'PC_088', 'PC_054', 'PC_019',
                                   'PC_091', 'PC_057', 'PC_089', 'PC_112', 'PC_055', 'PC_038', 'PC_059', 'PC_009',
                                   'PC_071', 'PC_056', 'PC_098', 'PC_040', 'PC_051', 'PC_080', 'PC_041', 'PC_084',
                                   'PC_114', 'PC_006', 'PC_044', 'PC_090', 'PC_045', 'PC_087', 'PC_096', 'PC_026',
                                   'PC_060', 'PC_046', 'PC_022', 'PC_081', 'PC_037', 'PC_012', 'PC_043', 'PC_017',
                                   'PC_020', 'PC_050', 'PC_049', 'PC_021', 'PC_033', 'PC_069', 'PC_101', 'PC_082',
                                   'PC_010', 'PC_079', 'PC_113', 'PC_028', 'PC_116', 'PC_076', 'PC_092', 'PC_011',
                                   'PC_072', 'PC_062', 'PC_007', 'PC_075', 'PC_074', 'PC_073', 'PC_029', 'PC_085',
                                   'PC_014', 'PC_095', 'PC_015', 'PC_099', 'PC_083', 'PC_100', 'PC_078', 'PC_018',
                                   'PC_097', 'PC_999', 'PC_077', 'PC_104', 'PC_031', 'L0_5001_F', 'L0_5003_F',
                                   'PC_117', 'L0_5002_F', 'L0_5004_F']
        leafs = tm1.elements.get_leaf_element_names('CUST_Plan_Full', 'CUST_Plan_Full')
        leafs = ['PC_999']
        # leafs = ['PCEXP006']
        #leafs = from_largest_to_smaller
        logger.info(f'selected customers: {leafs}')
        logger.info(f"{'=' * 100}")

        load_supporting_attributes_and_cubes(tm1)
        start = time.time()
        tasks_info = []
        cell_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            tasks = [executor.submit(worker, tm1, leaf) for leaf in leafs]
        for task in concurrent.futures.as_completed(tasks):
            task_info = task.result()
            tasks_info.append(task_info)
            cell_count += task_info['count']
        failed_customers = [k['customer'] for k in tasks_info if k['status'] == 'FAIlED']
        logger.info(f'time to complete {time.time() - start} - {cell_count} cells downloaded ')
        if failed_customers:
            logger.info(f'the following customers failed to download {",".join(failed_customers)}')
        df = pd.DataFrame(tasks_info)
        df.set_index(['thread_id', 'status'], inplace=True)
        df.sort_index(inplace=True)
        df.to_csv(f'Performances/{performance_file_name}.csv')
        return tasks_info


if __name__ == '__main__':

    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    time_stamp = time.strftime("%Y-%m-%d %H:%M:%S", t)
    performance_file_name = time.strftime("%Y-%m-%d_%H-%M-%S", t)

    n_runs = 1
    runs = {'number_of_runs': n_runs, 'runs': []}
    with open('Downloads/runs.json', 'w') as fp:
        fp.write('{"number_of_runs": ' + str(n_runs) + ', "runs": [')
        for i in range(n_runs):
            print(f'run #: {i}')
            start = time.time()
            try:
                tasks_info = main()
            except Exception as e:
                logger.error(e)
                break
            end_time = time.time()
            total_time = f'{end_time - start}'
        
            to_write = {'run': i,
                        'total_time': total_time,
                        'start_time': time.asctime( time.localtime(start)),
                        'end_time': time.asctime( time.localtime(end_time)),
                        'infos': [{'customer': task['customer'], 'count': task['count'], 'tries': task['tries']} for task in tasks_info if task['tries'] > 1]}
            fp.write(json.dumps(to_write))

            if i < n_runs - 1:
                fp.write(',')
            
            logger.info(f'total time to complete {total_time}')

        fp.write(']}')



    ''' --- VERSIONE DUMP INTERO ---

    with open('Downloads/runs.json', 'w') as fp:
        for i in range(n_runs):
            start = time.time()
            tasks_info = main()
            end_time = time.time()
            total_time = f'{end_time - start}'
            runs['runs'].append({'run': i,
                                'total_time': total_time,
                                'start_time': time.asctime( time.localtime(start)),
                                'end_time': time.asctime( time.localtime(end_time)),
                                'infos': [{'customer': task['customer'], 'count': task['count'], 'tries': task['tries']} for task in tasks_info]})
            logger.info(f'total time to complete {total_time}')
        with open('Downloads/runs.json', 'w') as fp:
            json.dump(runs, fp)
    '''
