# CS-E4780 Scalable Systems and Data Management Course Project
## Efficient Pattern Detection over Data Stream

### Authors

**Linh Van Nguyen**  
Aalto University, Espoo, Finland  
📧 linh.10.nguyen@aalto.fi

**Prof. Bo Zhao** (Supervisor, Responsible Teacher)  
Aalto University, Espoo, Finland  
📧 bo.zhao@aalto.fi

### 📄 Project Report

[**Read the Full Report (PDF)**](./Efficient_Pattern_Detection_over_Data_Stream-4.pdf)

---

## OpenCEP - Load Shedding for Complex Event Processing

This repository contains an implementation of load shedding strategies for Complex Event Processing (CEP) on real-world data.

## About OpenCEP

OpenCEP is a framework for pattern detection in event streams. For detailed framework documentation, see the original repository: https://github.com/ilya-kolchinsky/OpenCEP.git

## Load Shedding Implementation

This project implements **6 load shedding strategies** for managing partial match storage in CEP systems:

1. **Simple/FIFO** - Remove oldest partial matches (O(n-excess) complexity)
2. **Efficient** - Adaptive batch removal to reduce shedding frequency (O(n log n))
3. **Batched** - Hysteresis mechanism with 20% tolerance and 25% reduction
4. **Random** - Uniform probability distribution for removal
5. **Priority** - Remove lowest probability matches first

### Implementation Files

- **Load Shedding Strategies**: `tree/PatternMatchStorage.py`
- **Configuration Settings**: `misc/DefaultConfig.py`
- **Evaluation Script**: `main.py`

## Evaluation on 2018 NYC Citi Bike Dataset

The system evaluates load shedding strategies using real 2018 NYC Citi Bike trip data.

### Query Pattern

Hot Path Query detecting bike trips ending at popular stations:
```
PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
WHERE a[i+1].bike = a[i].bike AND b.end in {hot_stations}
AND a[last].bike = b.bike AND a[i+1].start = a[i].end
WITHIN 1h
```

### Running the Evaluation

1. **Download the data**:
   ```bash
   bash data.install.sh
   ```

2. **Run the evaluation**:
   ```bash
   python3 main.py
   ```

## Configuration

Modify load shedding behavior in `misc/DefaultConfig.py`:

```python
LOAD_SHEDDING_ENABLED = True  # Enable/disable load shedding
LOAD_SHEDDING_STRATEGY = "fifo"  # Strategy: simple, efficient, fifo, batched, random, priority
MAX_PARTIAL_MATCHES = 1000  # Maximum partial matches before shedding
```

## References

1. **Agrawal, J., Diao, Y., Gyllstrom, D., & Immerman, N.** (2008). Efficient pattern matching over event streams. In *Proceedings of the 2008 ACM SIGMOD international conference on Management of data* (pp. 147–160). https://doi.org/10.1145/1376616.1376634

2. **Apache Software Foundation** (2025). FlinkCEP - Complex event processing for Flink. https://nightlies.apache.org/flink/flink-docs-master/docs/libs/cep/

3. **Citi Bike** (2025). Citi Bike System Data. https://citibikenyc.com/system-data

4. **Cugola, G., & Margara, A.** (2012). Processing flows of information: From data stream to complex event processing. *ACM Computing Surveys (CSUR)*, 44(3), 1–62. https://doi.org/10.1145/2187671.2187677

5. **Databricks** (2024). What is Complex Event Processing [CEP]? https://www.databricks.com/glossary/complex-event-processing

6. **Kolchinsky, I.** (2025). OpenCEP: Complex Event Processing Engine. https://github.com/ilya-kolchinsky/OpenCEP

7. **Redpanda Data** (2024). Complex event processing—Architecture and other practical considerations. https://www.redpanda.com/guides/event-stream-processing-complex-event-processing

8. **Yu, C., Shi, T., Weidlich, M., & Zhao, B.** (2025). SHARP: Shared State Reduction for Efficient Matching of Sequential Patterns. *arXiv preprint arXiv:2507.04872*. https://arxiv.org/abs/2507.04872

9. **Zhao, B., Hung, N. Q. V., & Weidlich, M.** (2020). Load shedding for complex event processing: Input-based and state-based techniques. In *2020 IEEE 36th International Conference on Data Engineering (ICDE)* (pp. 1093–1104). IEEE. https://doi.org/10.1109/ICDE48307.2020.00099

10. **Zhao, B., Shi, T., & Weidlich, M.** (2023). Efficient evaluation of complex event processing queries over massive data streams. *IEEE Transactions on Knowledge and Data Engineering*, 35(8), 8234–8247. https://doi.org/10.1109/TKDE.2022.3206509

---

## 📄 License

This project is based on the OpenCEP framework. Please refer to the original repository for license information.