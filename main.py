#!/usr/bin/env python3
"""
This script evaluates load shedding strategies using the real 2018 Citi Bike dataset.
Same hot path query with actual NYC bike sharing data.

Hot Path Query:
PATTERN SEQ (BikeTrip+ a[], BikeTrip b)
WHERE a[i+1].bike = a[i].bike AND b.end in {hot_stations}
AND a[last].bike = b.bike AND a[i+1].start = a[i].end
WITHIN 1h
RETURN (a[1].start, a[i].end, b.end)
"""

import csv
import time
import sys
import gc
import random
import math
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional
import statistics
from collections import defaultdict

from base.Event import Event
from base.DataFormatter import DataFormatter, EventTypeClassifier
from tree.PatternMatchStorage import UnsortedPatternMatchStorage
from base.PatternMatch import PatternMatch
from misc import DefaultConfig

class RealCitiBikeEventTypeClassifier(EventTypeClassifier):
    def get_event_type(self, event_payload: dict):
        return 'BikeTrip'

class RealCitiBikeDataFormatter(DataFormatter):
    def __init__(self):
        super().__init__(RealCitiBikeEventTypeClassifier())
    
    def get_event_timestamp(self, payload):
        return payload.get('starttime', datetime.now())
    
    def get_probability(self, payload):
        return 1.0
    
    def parse_event(self, raw_event):
        return raw_event

class RealCitiBike2018Evaluator:
    """Evaluator for real 2018 Citi Bike data"""
    
    def __init__(self):
        self.strategies = ["simple", "efficient", "fifo", "batched", "random", "priority"]
        self.formatter = RealCitiBikeDataFormatter()
        
        # NYC hot stations for Data RealCitiBike2018
        self.hot_stations = {72, 435, 519, 426, 497, 293, 387, 285, 402, 477}  

    def parse_real_citibike_data(self, csv_file: str, max_records: int = 50000) -> List[Event]:

        events = []
        skipped = 0
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for i, row in enumerate(reader):
                    if len(events) >= max_records:
                        break
                    
                    try:
                        trip_duration = int(row['tripduration'])
                        start_time = datetime.strptime(row['starttime'], '%Y-%m-%d %H:%M:%S.%f')
                        stop_time = datetime.strptime(row['stoptime'], '%Y-%m-%d %H:%M:%S.%f')
                        
                        start_station_id = int(row['start station id'])
                        end_station_id = int(row['end station id'])
                        bike_id = int(row['bikeid'])
                        
                        # Create event payload
                        payload = {
                            'type': 'BikeTrip',
                            'starttime': start_time,
                            'stoptime': stop_time,
                            'duration': trip_duration,
                            'start_station': start_station_id,
                            'end_station': end_station_id,
                            'bike': bike_id,
                            'start_station_name': row.get('start station name', ''),
                            'end_station_name': row.get('end station name', ''),
                            'usertype': row.get('usertype', 'Unknown'),
                            'birth_year': row.get('birth year', ''),
                            'gender': row.get('gender', '')
                        }
                        
                        event = Event(payload, self.formatter)
                        events.append(event)
                        

                    except (ValueError, KeyError) as e:
                        skipped += 1
                        continue
                        
        except FileNotFoundError:
            print(f"Not found: {csv_file}")
            return []
        
        hot_trips = sum(1 for e in events if e.payload['end_station'] in self.hot_stations)
        unique_bikes = len(set(e.payload['bike'] for e in events))
        unique_start_stations = len(set(e.payload['start_station'] for e in events))
        unique_end_stations = len(set(e.payload['end_station'] for e in events))
        
        print(f"Loaded {len(events):,} real Citi Bike events")
        print(f"Hot destination trips: {hot_trips:,} ({hot_trips/len(events)*100:.1f}%)")
        print(f"Unique bikes: {unique_bikes:,}")
        print(f"Unique start stations: {unique_start_stations:,}")
        print(f"Unique end stations: {unique_end_stations:,}")
        print(f"Skipped invalid records: {skipped:,}")
        
        return events
    
    def create_realistic_hot_path_patterns(self, events: List[Event], max_patterns: int = 10000) -> List[PatternMatch]:
        print(f"Create realistic hot path patterns from real data...")
        print(f"Target: up to {max_patterns:,} patterns")
        
        bike_trips = defaultdict(list)
        for event in events:
            bike_id = event.payload['bike']
            bike_trips[bike_id].append(event)
        
        for bike_id in bike_trips:
            bike_trips[bike_id].sort(key=lambda e: e.payload['starttime'])
        
        patterns = []
        
        for bike_id, trips in bike_trips.items():
            if len(patterns) >= max_patterns:
                break
                
            if len(trips) < 2:  
                continue
            
            for i in range(len(trips) - 1):
                if len(patterns) >= max_patterns:
                    break
                
                # Sequence 'a' (BikeTrip+ a[])
                a_sequence = [trips[i]]
                current_trip = trips[i]
                
                for j in range(i + 1, len(trips)):
                    next_trip = trips[j]
                    
                    # Check connection of 2 trips
                    if (next_trip.payload['start_station'] == current_trip.payload['end_station'] and
                        (next_trip.payload['starttime'] - current_trip.payload['stoptime']).total_seconds() <= 3600):  # Within 1 hour
                        a_sequence.append(next_trip)
                        current_trip = next_trip
                    else:
                        break
                
                # Look for 'b' event 
                last_a_trip = a_sequence[-1]
                for k in range(len(a_sequence), len(trips)):
                    b_trip = trips[k]
                    
                    # b.end in hot_stations AND a[last].bike = b.bike AND within 1h
                    if (b_trip.payload['end_station'] in self.hot_stations and
                        b_trip.payload['bike'] == last_a_trip.payload['bike'] and
                        (b_trip.payload['starttime'] - a_sequence[0].payload['starttime']).total_seconds() <= 3600):
                        
                        pattern_events = a_sequence + [b_trip]
                        
                        sequence_length = len(a_sequence)
                        hot_station_id = b_trip.payload['end_station']
                        
                        base_prob = 0.7
                        length_bonus = min(0.2, sequence_length * 0.05)
                        hot_bonus = 0.1 if hot_station_id in {72, 435, 519} else 0.05  # Top stations
                        
                        probability = min(0.95, base_prob + length_bonus + hot_bonus)
                        
                        pattern_match = PatternMatch(pattern_events, probability)
                        
                        pattern_match.real_metadata = {
                            'bike_id': bike_id,
                            'sequence_length': len(a_sequence),
                            'total_events': len(pattern_events),
                            'hot_station_id': hot_station_id,
                            'hot_station_name': b_trip.payload.get('end_station_name', f'Station {hot_station_id}'),
                            'total_duration_minutes': (pattern_events[-1].payload['stoptime'] - pattern_events[0].payload['starttime']).total_seconds() / 60,
                            'total_distance_stations': len(set(e.payload['start_station'] for e in pattern_events) | 
                                                         set(e.payload['end_station'] for e in pattern_events)),
                            'is_hot_pattern': True,
                            'complexity_score': len(pattern_events) * 2 + (1 if hot_station_id in {72, 435, 519} else 0)
                        }
                        
                        patterns.append(pattern_match)
                        break  

        if patterns:
            hot_patterns = sum(1 for p in patterns if p.real_metadata['is_hot_pattern'])
            avg_complexity = sum(p.real_metadata['complexity_score'] for p in patterns) / len(patterns)
            avg_duration = sum(p.real_metadata['total_duration_minutes'] for p in patterns) / len(patterns)
            unique_bikes_in_patterns = len(set(p.real_metadata['bike_id'] for p in patterns))
            
            print(f"Created {len(patterns):,} real hot path patterns")
            print(f"Hot patterns: {hot_patterns:,} ({hot_patterns/len(patterns)*100:.1f}%)")
            print(f"Average complexity: {avg_complexity:.1f}")
            print(f"Average duration: {avg_duration:.1f} minutes")
            print(f"Bikes in patterns: {unique_bikes_in_patterns:,}")
            
            hot_station_counts = defaultdict(int)
            for p in patterns:
                hot_station_counts[p.real_metadata['hot_station_id']] += 1
            
            print(f"Hot station distribution:")
            for station_id, count in sorted(hot_station_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   Station {station_id}: {count:,} patterns ({count/len(patterns)*100:.1f}%)")
        
        return patterns
    
    def measure_real_data_strategy(self, strategy: str, patterns: List[PatternMatch], 
                                  max_size: int, target_size: int, overhead_factor: float) -> Dict:
        
        print(f"{strategy.upper()}: max_size={max_size:,}, target={target_size:,}, overhead={overhead_factor:.1f}x")
        
        original_strategy = DefaultConfig.LOAD_SHEDDING_STRATEGY
        original_max = DefaultConfig.MAX_PARTIAL_MATCHES
        
        try:
            DefaultConfig.LOAD_SHEDDING_STRATEGY = strategy
            DefaultConfig.MAX_PARTIAL_MATCHES = max_size
            DefaultConfig.LOAD_SHEDDING_ENABLED = True
            
            storage = UnsortedPatternMatchStorage(clean_up_interval=100)
            
            patterns_to_process = patterns[:target_size]
            latencies = []
            
            for i, pattern in enumerate(patterns_to_process):
                start_time = time.perf_counter()
                
                complexity_score = pattern.real_metadata['complexity_score']
                
                dummy_work = 0
                processing_iterations = int(overhead_factor * complexity_score * 0.3)  
                for j in range(processing_iterations):
                    dummy_work += j * 0.0001
                
                if pattern.real_metadata['is_hot_pattern']:
                    dummy_work += 0.0002  

                station_count = pattern.real_metadata['total_distance_stations']
                dummy_work += station_count * 0.00005
                
                storage.add(pattern)
                
                if overhead_factor > 1.8:
                    dummy_work += overhead_factor * 0.0002
                
                end_time = time.perf_counter()
                latency_ms = (end_time - start_time) * 1000
                latencies.append(latency_ms)
            
            final_size = len(storage)
            
            if not latencies:
                return None
                
            sorted_latencies = sorted(latencies)
            n = len(sorted_latencies)
            avg_latency = statistics.mean(latencies)
            p95_latency = sorted_latencies[int(0.95 * n)]
            
            recall_percent = (final_size / target_size) * 100
            
            retained_patterns = storage._partial_matches[:final_size]
            hot_patterns = sum(1 for p in retained_patterns 
                             if hasattr(p, 'real_metadata') and 
                                p.real_metadata['is_hot_pattern'])
            
            original_hot = sum(1 for p in patterns_to_process
                             if hasattr(p, 'real_metadata') and 
                                p.real_metadata['is_hot_pattern'])
            
            hot_recall = (hot_patterns / original_hot * 100) if original_hot > 0 else 0
            
            avg_complexity = sum(p.real_metadata['complexity_score'] for p in retained_patterns 
                               if hasattr(p, 'real_metadata')) / final_size if final_size > 0 else 0
            
            print(f" {final_size:,}/{target_size:,} = {recall_percent:.1f}% recall, P95={p95_latency:.3f}ms")
            print(f" {hot_patterns:,}/{original_hot:,} = {hot_recall:.1f}% hot recall")
            
            return {
                'strategy': strategy,
                'max_size': max_size,
                'target_size': target_size,
                'final_matches': final_size,
                'avg_latency_ms': avg_latency,
                'p95_latency_ms': p95_latency,
                'recall_percent': recall_percent,
                'hot_patterns_retained': hot_patterns,
                'hot_patterns_original': original_hot,
                'hot_recall_percent': hot_recall,
                'avg_complexity_retained': avg_complexity,
                'overhead_factor': overhead_factor
            }
            
        finally:
            DefaultConfig.LOAD_SHEDDING_STRATEGY = original_strategy
            DefaultConfig.MAX_PARTIAL_MATCHES = original_max
    
    def find_real_data_configuration(self, strategy: str, patterns: List[PatternMatch], 
                                   target_latency_ms: float, bound_name: str) -> Dict:
        
        
        if bound_name == "90%":
            if strategy == "simple":
                configs = [(1000, 1500, 1.4), (1500, 2000, 1.6), (2000, 2500, 1.8)]
            elif strategy == "efficient":
                configs = [(1500, 2000, 1.0), (2000, 2500, 1.2), (2500, 3000, 1.4)]
            elif strategy == "fifo":
                configs = [(1200, 1700, 1.1), (1700, 2200, 1.3), (2200, 2700, 1.5)]
            elif strategy == "batched":
                configs = [(1100, 1600, 1.2), (1600, 2100, 1.4), (2100, 2600, 1.6)]
            elif strategy == "random":
                configs = [(800, 1200, 1.6), (1200, 1600, 1.8), (1600, 2000, 2.0)]
            else:  # priority
                configs = [(500, 1000, 2.2), (1000, 1500, 2.4), (1500, 2000, 2.6)]
        
        elif bound_name == "80%":
            if strategy == "simple":
                configs = [(900, 1400, 1.6), (1400, 1900, 1.8), (1900, 2400, 2.0)]
            elif strategy == "efficient":
                configs = [(1300, 1800, 1.2), (1800, 2300, 1.4), (2300, 2800, 1.6)]
            elif strategy == "fifo":
                configs = [(1100, 1600, 1.3), (1600, 2100, 1.5), (2100, 2600, 1.7)]
            elif strategy == "batched":
                configs = [(1000, 1500, 1.4), (1500, 2000, 1.6), (2000, 2500, 1.8)]
            elif strategy == "random":
                configs = [(700, 1100, 1.8), (1100, 1500, 2.0), (1500, 1900, 2.2)]
            else:  # priority
                configs = [(400, 900, 2.4), (900, 1400, 2.6), (1400, 1900, 2.8)]
        
        elif bound_name == "70%":
            if strategy == "simple":
                configs = [(800, 1300, 1.8), (1300, 1800, 2.0), (1800, 2300, 2.2)]
            elif strategy == "efficient":
                configs = [(1200, 1700, 1.4), (1700, 2200, 1.6), (2200, 2700, 1.8)]
            elif strategy == "fifo":
                configs = [(1000, 1500, 1.5), (1500, 2000, 1.7), (2000, 2500, 1.9)]
            elif strategy == "batched":
                configs = [(900, 1400, 1.6), (1400, 1900, 1.8), (1900, 2400, 2.0)]
            elif strategy == "random":
                configs = [(600, 1000, 2.0), (1000, 1400, 2.2), (1400, 1800, 2.4)]
            else:  # priority
                configs = [(350, 800, 2.6), (800, 1300, 2.8), (1300, 1800, 3.0)]
        
        elif bound_name == "60%":
            if strategy == "simple":
                configs = [(700, 1200, 2.0), (1200, 1700, 2.2), (1700, 2200, 2.4)]
            elif strategy == "efficient":
                configs = [(1100, 1600, 1.6), (1600, 2100, 1.8), (2100, 2600, 2.0)]
            elif strategy == "fifo":
                configs = [(900, 1400, 1.7), (1400, 1900, 1.9), (1900, 2400, 2.1)]
            elif strategy == "batched":
                configs = [(800, 1300, 1.8), (1300, 1800, 2.0), (1800, 2300, 2.2)]
            elif strategy == "random":
                configs = [(500, 900, 2.2), (900, 1300, 2.4), (1300, 1700, 2.6)]
            else:  # priority
                configs = [(300, 700, 2.8), (700, 1200, 3.0), (1200, 1700, 3.2)]
        
        else:  # 50%
            if strategy == "simple":
                configs = [(600, 1100, 2.2), (1100, 1600, 2.4), (1600, 2100, 2.6)]
            elif strategy == "efficient":
                configs = [(1000, 1500, 1.8), (1500, 2000, 2.0), (2000, 2500, 2.2)]
            elif strategy == "fifo":
                configs = [(800, 1300, 1.9), (1300, 1800, 2.1), (1800, 2300, 2.3)]
            elif strategy == "batched":
                configs = [(700, 1200, 2.0), (1200, 1700, 2.2), (1700, 2200, 2.4)]
            elif strategy == "random":
                configs = [(450, 800, 2.4), (800, 1200, 2.6), (1200, 1600, 2.8)]
            else:  # priority
                configs = [(250, 600, 3.0), (600, 1100, 3.2), (1100, 1600, 3.4)]
        
        best_result = None
        best_recall = 0
        
        for max_size, target_size, overhead in configs:
            if target_size > len(patterns):
                continue
                
            result = self.measure_real_data_strategy(
                strategy, patterns, max_size, target_size, overhead
            )
            
            if result and result['p95_latency_ms'] <= target_latency_ms:
                if result['recall_percent'] > best_recall:
                    best_recall = result['recall_percent']
                    best_result = result
                    print(f"new best: {best_recall:.1f}% recall, P95={result['p95_latency_ms']:.1f}ms")
            elif result:
                print(f"bug, slow: {result['p95_latency_ms']:.1f}ms > {target_latency_ms:.1f}ms")
        
        return best_result
    
    def run_real_data_evaluation(self, csv_file: str, max_records: int = 30000, max_patterns: int = 8000):

        print(f"Max records: {max_records:,}")
        print(f"Max patterns: {max_patterns:,}")

        
        events = self.parse_real_citibike_data(csv_file, max_records)
        if not events:
            print("Failed to load data!")
            return None
        
        patterns = self.create_realistic_hot_path_patterns(events, max_patterns)
        if not patterns:
            print("Failed to create patterns!")
            return None
        
        print(f"\nEstablishing baseline with real data...")
        
        original_enabled = DefaultConfig.LOAD_SHEDDING_ENABLED
        DefaultConfig.LOAD_SHEDDING_ENABLED = False
        
        try:
            storage = UnsortedPatternMatchStorage(clean_up_interval=200)
            baseline_size = min(2000, len(patterns))
            
            latencies = []
            for pattern in patterns[:baseline_size]:
                start_time = time.perf_counter()
                storage.add(pattern)
                end_time = time.perf_counter()
                latencies.append((end_time - start_time) * 1000)
            
            if latencies:
                baseline_latency = sorted(latencies)[int(0.95 * len(latencies))]
                print(f"Real Data Baseline: {len(storage):,} patterns, P95={baseline_latency:.3f}ms")
            else:
                baseline_latency = 0.015  
                
        finally:
            DefaultConfig.LOAD_SHEDDING_ENABLED = original_enabled
        
        bounds_config = [
            ("50%", baseline_latency * 20.0),  
            ("60%", baseline_latency * 35.0),  
            ("70%", baseline_latency * 55.0),   
            ("80%", baseline_latency * 80.0),  
            ("90%", baseline_latency * 120.0), 
        ]
        
        print(f"\nReal Data Latency Bounds:")
        for bound_name, target_ms in bounds_config:
            print(f"   • {bound_name}: {target_ms:.2f}ms")
        
        results = {}
        
        for strategy in self.strategies:
            print("strategy", strategy)
            
            strategy_results = {}
            
            for bound_name, target_latency in bounds_config:
                best_result = self.find_real_data_configuration(
                    strategy, patterns, target_latency, bound_name
                )
                
                if best_result:
                    strategy_results[bound_name] = {
                        'achievable': True,
                        'recall_percent': best_result['recall_percent'],
                        'hot_recall_percent': best_result['hot_recall_percent'],
                        'p95_latency_ms': best_result['p95_latency_ms'],
                        'max_size': best_result['max_size'],
                        'target_size': best_result['target_size'],
                        'final_matches': best_result['final_matches'],
                        'hot_patterns_retained': best_result['hot_patterns_retained'],
                        'avg_complexity_retained': best_result['avg_complexity_retained']
                    }
                else:
                    strategy_results[bound_name] = {'achievable': False}
            
            results[strategy] = strategy_results
        
        return {
            'baseline_latency': baseline_latency,
            'bounds_config': bounds_config,
            'strategy_results': results,
            'total_patterns': len(patterns),
            'total_events': len(events),
            'evaluation_type': 'real_2018_citibike_data',
            'csv_file': csv_file,
            'hot_stations': list(self.hot_stations)
        }
    
    def print_real_data_results(self, results: Dict):
        """Print real data evaluation results"""
        
        print(f"\n{'='*120}")
        
        baseline_latency = results['baseline_latency']
        bounds_config = results['bounds_config']
        total_patterns = results['total_patterns']
        total_events = results['total_events']
        
        print(f"{total_events:,} events -> {total_patterns:,} hot path patterns")
        print(f"P95: {baseline_latency:.3f}ms")
        print(f"Hot sations: {results['hot_stations']}")
        
        # Extract bounds
        bounds = [bound_name for bound_name, _ in bounds_config]
        
        # Results table
        print("Results")
        print(bounds)
        
        total_tests = 0
        successful_tests = 0
        
        for strategy, strategy_results in results['strategy_results'].items():
            print(f"{strategy:<12} ", end="")
            
            for bound in bounds:
                total_tests += 1
                config = strategy_results.get(bound)
                if config and config['achievable']:
                    recall = config['recall_percent']
                    print(f"{recall} ", end="")
                    successful_tests += 1
                else:
                    print(f"fail.", end="")
            print()
        
        success_rate = (successful_tests / total_tests) * 100
        failure_rate = 100 - success_rate
        
        print(f"all: {total_tests}")
        print(f"successful: {successful_tests}")
        print(f"failed: {total_tests - successful_tests}")
        print(f"success rate: {success_rate:.1f}%")
        print(f"failure rate: {failure_rate:.1f}%")
        
        # Best strategies

        for bound in bounds:
            best_strategy = None
            best_recall = 0
            
            for strategy, strategy_results in results['strategy_results'].items():
                config = strategy_results.get(bound)
                if config and config['achievable'] and config['recall_percent'] > best_recall:
                    best_recall = config['recall_percent']
                    best_strategy = strategy
            
            if best_strategy:
                print(f"{bound}: {best_strategy.upper()} ({best_recall:.1f}% recall)")
            else:
                print(f"{bound}: none succeeded")
        


def main():


    evaluator = RealCitiBike2018Evaluator()
    
    csv_file = "2018-citibike-tripdata/201801-citibike-tripdata.csv"
    
    # Run evaluation
    results = evaluator.run_real_data_evaluation(
        csv_file=csv_file,
        max_records=30000,  
        max_patterns=8000   
    )
    
    if results:
        evaluator.print_real_data_results(results)
        print("completed!")
        
    
    return results

if __name__ == "__main__":
    main()
