def job_mode(server):
    """
    This function tells Delphix how to execute jobs, based on the single_thread variable at the beginning of the file
    """
    #Synchronously (one at a time)
    if single_thread == True:
        job_m = job_context.sync(server)
        print_debug("These jobs will be executed synchronously")
    #Or asynchronously
    else:
        job_m = job_context.async(server)
        print_debug("These jobs will be executed asynchronously")
    return job_m


def job_wait():
    """
    This job stops all work in the thread/process until jobs are completed.
    """
    #Grab all the jos on the server (the last 25, be default)
    all_jobs = job.get_all(server)
    #For each job in the list, check to see if it is running (not ended)
    for jobobj in all_jobs:
        if not (jobobj.job_state in ["CANCELED", "COMPLETED", "FAILED"]):
            print_debug("Waiting for " + jobobj.reference + " (currently: " + jobobj.job_state+ ") to finish running against the container")
            #If so, wait
            job_context.wait(server,jobobj.reference)
