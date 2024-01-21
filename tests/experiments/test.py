from mpservice.multiprocessing.remote_exception import RemoteException
import pickle

def main():
    try:
        raise ValueError(38)
    except Exception as e:
        exc = RemoteException(e)

        ee = pickle.loads(pickle.dumps(exc))

        # raise ee

        zz = pickle.loads(pickle.dumps(exc))
        raise zz
    

if __name__ == '__main__':
    main()
