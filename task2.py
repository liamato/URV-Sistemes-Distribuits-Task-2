#! /usr/bin/env python3
import time
import pywren_ibm_cloud as pywren
import cos
import sys



MASTER_DELAY = .002
SLAVES_DELAY = .004
BUCKET_NAME = 'test-buck-urv'

N_SLAVES = 10

N_SAMPLES = 10

DEBUG = 1

pywren_config = {
    'pywren' : {'storage_bucket' : BUCKET_NAME},

    'ibm_cf': {
        'endpoint': '', 
        'namespace': '', 
        'api_key': ''
    }, 

    'ibm_cos': {
        'endpoint': '',
        'secret_key': '',
        'access_key': '',

        'api_key': '',
        'private_endpoint': ''
    }
}

def getRequests(cos):
	requests = sorted(cos.list_objects(prefix="p_write_"), key=lambda e: e['LastModified'])
	return list(map(lambda e: e['Key'].split('p_write_')[1], requests))

# 1. monitor COS bucket each X seconds
# 2. List all "p_write_{id}" files
# 3. Order objects by time of creation
# 4. Pop first object of the list "p_write_{id}"
# 5. Write empty "write_{id}" object into COS
# 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
# 7. Monitor "result.json" object each X seconds until it is updated
# 8. Delete from COS “write_{id}”
# 8. Back to step 1 until no "p_write_{id}" objects in the bucket
def master(id, x, buck, ibm_cos):
	write_permission_list = []

	backend = cos.Backend(ibm_cos, buck)

	x = max(x, 0.01)

	backend.put_object('result.txt', b'')
	updated = backend.get_etag('result.txt')

	time.sleep(x)
	requests = getRequests(backend)


	while len(requests) > 0:
		request = requests.pop(0)

		tmp = backend.get_etag('result.txt')

		backend.delete_object('p_write_{}'.format(request))
		write_permission_list.append(request)
		backend.put_object('write_{}'.format(request), '')


		timeoutCounter = 0
		while tmp == updated:
			print("Wait")
			time.sleep(x)
			tmp = backend.get_etag('result.txt')
			timeoutCounter += 1

			if timeoutCounter > 200:
				write_permission_list.append(-1)
				return write_permission_list


		updated = tmp
		backend.delete_object('write_{}'.format(request))

		time.sleep(x)
		requests = getRequests(backend)

	
	return write_permission_list





# 1. Write empty "p_write_{id}" object into COS
# 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
# 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
# 4. Finish

# No need to return anything
def slave(id, x, buck, ibm_cos):

	backend = cos.Backend(ibm_cos, buck)

	backend.put_object('p_write_{}'.format(id), '')

	while not backend.exist_object('write_{}'.format(id)):
		time.sleep(x)

	t1 = time.time()
	idStr = '{}\n'.format(id)
	contents = backend.get_object('result.txt', False).decode('utf-8')

	if len(contents) == 0 or '\n'+idStr not in contents:
		contents += idStr

	backend.put_object('result.txt', contents)
	t2 = time.time()

	print(t2-t1,'s')



def execute(executor, cos_backend, slaves, master_delay, slave_delay):

	t1 = time.time()
	executor.map(slave, [slave_delay] * slaves, timeout=max(600, max(master_delay, slave_delay)*slaves*3), extra_params=[cos_backend.bucket_name])
	
	future = executor.call_async(master, [master_delay, cos_backend.bucket_name], timeout=max(600, max(master_delay, slave_delay)*slaves*3))


	#--------------- DEBUG ---------------#
	if DEBUG > 1:
		time.sleep(master_delay * 2)

		requests = getRequests(cos_backend)

		while len(requests) > 0:
			print(requests, cos_backend.get_etag('result.txt'))
			time.sleep(master_delay)
			requests = getRequests(cos_backend)
	#--------------- DEBUG ---------------#


	write_permission_list = executor.get_result(future)

	t2 = time.time()

	res_time = t2 - t1


	results = cos_backend.get_object('result.txt').decode('utf-8').split('\n')

	results = list(filter(lambda e: e != '', results))


	write_permission_list = list(map(int, write_permission_list))

	results = list(map(int, results))


	#--------------- DEBUG ---------------#
	if DEBUG > 0:
		print()
		print('   expected:', write_permission_list)
		print('results.txt:', results)
		print()
	#--------------- DEBUG ---------------#



	same = (write_permission_list == results)

	return same, res_time


def main():
	path = None
	if len(sys.argv) >= 2:
		path = sys.argv[1]


	backend = cos.Backend(pywren_config['ibm_cos'], pywren_config['pywren']['storage_bucket'])
	pw = pywren.ibm_cf_executor(config=pywren_config)


	if path == None:
		execute(pw, backend, N_SLAVES, MASTER_DELAY, SLAVES_DELAY)
		return


	num_slaves = N_SLAVES
	num_samples = N_SAMPLES

	if len (sys.argv) >= 3:
		num_slaves = int(sys.argv[2])

	if len (sys.argv) >= 4:
		num_samples = int(sys.argv[3])
	
	with open(path, "w") as f:
		f.write("slaves,time\n")
		for slaves in range(1, num_slaves+1, int(num_slaves/num_samples)):
			correct, execution_time = execute(pw, backend, slaves, MASTER_DELAY, SLAVES_DELAY)

			print(slaves, getRequests(backend))

			if not correct:
				print("ERROR: Incorrect execution sequence", file=sys.stderr)
				exit(1)

			f.write("{},{}\n".format(slaves, execution_time))


if __name__ == '__main__':
	main()
