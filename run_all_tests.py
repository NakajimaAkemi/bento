import os

algorithms = [ 'modin_ray','modin_dask']
              #'
              #'pyspark_pandas']
parts = ['4'] #, '2', '3', '4']S


save_output_results = False
max_num_tests = 1
modes = ['']   # the first is for core execution (default)

for part in parts:
    for mode in modes:
        for algorithm in algorithms:
            for i in range(max_num_tests):
                print(f'==================== algorithm {algorithm} - test {i} ===============================')
                print(f'mode {mode}')
                os.system(f'python run_algorithm.py --algorithm {algorithm} --dataset accidents --locally {mode}')
                if algorithm == 'modin_ray':
                    os.rename('pipeline_output/modin_loan_output.csv', 'pipeline_output/modin_ray_output.csv')

                if save_output_results:
                    if os.path.exists(f'pipeline_output/q{part}'):
                        os.system(f'rm -rf pipeline_output/q{part}')

                    os.mkdir(f'pipeline_output/q{part}')
                    os.system(f'mv pipeline_output/*.csv pipeline_output/q{part}')
