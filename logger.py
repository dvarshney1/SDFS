import logging

'''
Logger File that holds the code for logging/dumping into log file
'''

formatter= logging.Formatter('%(asctime)s - %(message)s')

def all_loggers(name, logging_file):

    handler = logging.FileHandler(logging_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    return logger


# class Logger:

#     def __init__(self, fileName):
#         self.fileName = fileName
#         # logging.basicConfig(filename= fileName, filemode='w', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level = logging.INFO)
#         self.messages = set()

#     def log(self, message):
#         logging.basicConfig(filename= self.fileName, filemode='w', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level = logging.INFO)
#         logging.info(message)
