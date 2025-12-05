#!/usr/bin/env python3
"""Script de test de charge pour la fonction get_timeline de Tiny Instagram.

Usage:
  python timeline.py --concurrent-users 10 --requests-per-user 5 --user-prefix user --output results.csv

Paramètres:
  --concurrent-users    Nombre d'utilisateurs distincts simulés (default: 50)
  --requests-per-user   Nombre de requêtes par utilisateur (default: 1)
  --user-prefix         Préfixe des noms d'utilisateurs (default: user)
  --limit               Limite de posts par timeline (default: 20)
  --output              Fichier CSV de sortie (default: conc.csv)
"""
import argparse
import time
import statistics
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import datastore
from datetime import datetime, timedelta

# Initialisation du client Datastore (partagé entre threads)
client = datastore.Client()


def get_timeline(user: str, limit: int = 20):
    """Copie de la fonction get_timeline fournie"""
    if not user:
        return []
    
    follow_key = client.key('User', user)
    user_entity = client.get(follow_key)
    follows = []
    if user_entity:
        follows = user_entity.get('follows', [])
    follows = list({*follows, user})

    timeline = []
    used_gql = False
    try:
        if hasattr(client, 'gql'):
            gql = client.gql("SELECT * FROM Post WHERE author IN @authors ORDER BY created DESC")
            gql.bindings["authors"] = follows
            timeline = list(gql.fetch(limit=limit))
            used_gql = True
    except Exception:
        pass
    
    if not used_gql:
        try:
            query = client.query(kind='Post')
            query.add_filter(filter=("author", "IN", follows))
            query.order = ['-created']
            timeline = list(query.fetch(limit=limit))
        except Exception:
            posts = []
            for author in follows:
                q = client.query(kind='Post')
                q.add_filter(filter=("author", "=", author))
                q.order = ['-created']
                posts.extend(list(q.fetch(limit=limit)))
            timeline = sorted(posts, key=lambda p: p.get('created'), reverse=True)[:limit]
    
    return timeline


def execute_timeline_request(user: str, limit: int):
    """Exécute une requête timeline et retourne le temps d'exécution."""
    start = time.time()
    try:
        result = get_timeline(user, limit)
        duration = time.time() - start
        return {
            'user': user,
            'duration': duration,
            'success': True,
            'posts_count': len(result),
            'error': None
        }
    except Exception as e:
        duration = time.time() - start
        return {
            'user': user,
            'duration': duration,
            'success': False,
            'posts_count': 0,
            'error': str(e)
        }


def run_user_requests(user: str, num_requests: int, limit: int):
    """Exécute plusieurs requêtes pour un utilisateur donné."""
    results = []
    
    # Requêtes mesurées
    for _ in range(num_requests):
        result = execute_timeline_request(user, limit)
        results.append(result)
    
    return results


def run_single_test(concurrent_users: int, requests_per_user: int, 
                    user_prefix: str, limit: int):
    """Lance un seul test de charge."""
    user_names = [f"{user_prefix}{i}" for i in range(1, concurrent_users + 1)]
    
    all_results = []
    test_start = time.time()
    
    # Exécution parallèle avec ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=concurrent_users) as executor:
        futures = {
            executor.submit(run_user_requests, user, requests_per_user, limit): user
            for user in user_names
        }
        
        completed = 0
        for future in as_completed(futures):
            user = futures[future]
            try:
                results = future.result()
                all_results.extend(results)
                completed += 1
            except Exception as e:
                print(f"Erreur pour {user}: {e}")
    
    test_duration = time.time() - test_start
    return all_results, test_duration


def run_load_test(concurrent_users: int, requests_per_user: int, 
                  user_prefix: str, limit: int, output_file: str, iterations: int = 3):
    """Lance le test de charge plusieurs fois."""
    print(f"\n{'='*70}")
    print(f"TEST DE CHARGE - Timeline Tiny Instagram")
    print(f"{'='*70}")
    print(f"Utilisateurs simultanés    : {concurrent_users}")
    print(f"Requêtes par utilisateur   : {requests_per_user}")
    print(f"Requêtes totales/test      : {concurrent_users * requests_per_user}")
    print(f"Limite de posts/timeline   : {limit}")
    print(f"Nombre d'itérations        : {iterations}")
    print(f"Fichier de sortie          : {output_file}")
    print(f"{'='*70}\n")
    

    # Exécuter le test plusieurs fois
    for iteration in range(1, iterations + 1):
        print(f"\nExécution {iteration}/{iterations}...")
        all_results, test_duration = run_single_test(
            concurrent_users, requests_per_user, user_prefix, limit
        )
        
        # Analyse et export des résultats
        print_results(all_results, test_duration, iteration)
        export_to_csv(all_results, concurrent_users, requests_per_user, limit, output_file)
        
        # Petite pause entre les itérations sauf pour la dernière
        if iteration < iterations:
            time.sleep(1)


def print_results(results: list, total_duration: float, iteration: int = 1):
    """Affiche les statistiques des résultats."""
    if not results:
        print("Aucun résultat à analyser.")
        return
    
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    if not successful:
        print("\nToutes les requêtes ont échoué!")
        for r in failed:
            print(f"   {r['user']}: {r['error']}")
        return
    
    durations = [r['duration'] for r in successful]
    posts_counts = [r['posts_count'] for r in successful]
    
    print(f"\n{'='*70}")
    print(f"RÉSULTATS - Itération {iteration}")
    print(f"{'='*70}")
    print(f"Durée totale du test       : {total_duration:.2f}s")
    print(f"Requêtes réussies          : {len(successful)}/{len(results)}")
    print(f"Requêtes échouées          : {len(failed)}")
    print(f"\nTEMPS DE RÉPONSE:")
    print(f"  Moyen                    : {statistics.mean(durations):.3f}s")
    
    if failed:
        print(f"\nERREURS ({len(failed)}):")
        error_summary = {}
        for r in failed:
            err = r['error']
            error_summary[err] = error_summary.get(err, 0) + 1
        for err, count in error_summary.items():
            print(f"  [{count}x] {err}")


def export_to_csv(results: list, concurrent_users: int, requests_per_user: int, 
                  limit: int, output_file: str):
    """Exporte les résultats dans un fichier CSV (une ligne par itération)."""
    if not results:
        print("\nAucun résultat à exporter.")
        return
    
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    # Calcul de la moyenne en millisecondes
    avg_time_ms = int(statistics.mean([r['duration'] * 1000 for r in successful])) if successful else 0
    
    # Paramètre de configuration du test
    param = concurrent_users
    
    # FAILED = 1 si au moins une requête a échoué, 0 sinon
    failed_flag = 1 if len(failed) > 0 else 0
    
    # Vérifier si le fichier existe
    file_exists = os.path.isfile(output_file)
    
    # Compter le numéro de RUN pour ce paramètre
    run_number = 1
    if file_exists:
        try:
            with open(output_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                # Trouver le dernier RUN pour ce paramètre
                matching_rows = [int(r['RUN']) for r in rows if r['PARAM'] == str(param)]
                if matching_rows:
                    run_number = max(matching_rows) + 1
        except Exception:
            pass
    
    # Écriture dans le CSV
    try:
        with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Écrire l'en-tête seulement si le fichier n'existe pas
            if not file_exists:
                writer.writerow(['PARAM', 'AVG_TIME', 'RUN', 'FAILED'])
            
            # Écrire une seule ligne pour cette itération
            writer.writerow([param, f"{avg_time_ms}ms", run_number, failed_flag])
        
        print(f"\nRésultats exportés vers {output_file}")
        print(f"  PARAM            : {param}")
        print(f"  AVG_TIME         : {avg_time_ms}ms")
        print(f"  RUN              : {run_number}")
        print(f"  FAILED           : {failed_flag}")
        print(f"  Requêtes réussies: {len(successful)}/{len(results)}")
        
    except Exception as e:
        print(f"\nErreur lors de l'export CSV: {e}")


def parse_args():
    p = argparse.ArgumentParser(
        description="Test de charge pour get_timeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument('--concurrent-users', type=int, default=50,
                   help='Nombre d\'utilisateurs distincts simulés')
    p.add_argument('--requests-per-user', type=int, default=1,
                   help='Nombre de requêtes par utilisateur')
    p.add_argument('--user-prefix', type=str, default='user',
                   help='Préfixe des noms d\'utilisateurs')
    p.add_argument('--limit', type=int, default=20,
                   help='Limite de posts par timeline')
    p.add_argument('--output', type=str, default='conc.csv',
                   help='Fichier CSV de sortie')
    return p.parse_args()


def main():
    args = parse_args()
    
    if args.concurrent_users <= 0 or args.requests_per_user <= 0:
        print("Erreur: concurrent-users et requests-per-user doivent être > 0")
        return
    
    run_load_test(
        concurrent_users=args.concurrent_users,
        requests_per_user=args.requests_per_user,
        user_prefix=args.user_prefix,
        limit=args.limit,
        output_file=args.output,
        iterations=3
    )


if __name__ == '__main__':
    main()