import time
import tunetools as tt

if __name__ == "__main__":
    def test(x):
        print(">>> start: " + str(x))
        time.sleep(10)
        print(">>> end")
        import random
        return {
            "result": x['alpha'] + x['beta'] + random.random() / 100,
            "result2": "this is result2"
        }


    def filter(x):
        return x['alpha'] != 0 and x['beta'] != 0

    search_spaces = [
        tt.GridSearchSpace("alpha", base_type=tt.Float, default=0.5, domain=[0.0, 0.3, 0.5]),
        tt.GridSearchSpace("beta", base_type=tt.Float, default=0.5, domain=[0.0, 0.3, 0.5]),
        tt.GridSearchSpace("gamma", base_type=tt.Float, default=4, domain=[4.0]),
        # GridSearchSpace("gamma", base_type=String, default="xxx", domain=["ff", "ddd"])
    ]

    # tt.run(obj_function=test, filter_function=filter, num_sample=20, parameters=search_spaces)
    # tt.test(obj_function=test, parameters=search_spaces, values={'alpha': 4})
    tt.test_or_run(obj_function=test, filter_function=filter, num_sample=20, parameters=search_spaces)

