from tqdm import tqdm
from pathlib import Path
import re
import json


def owndataset():
    data_path = Path('../data/raw')
    raw_train_path = data_path/'train'
    raw_valid_path = data_path/'dev'

    raw_train_paths = sorted(raw_train_path.iterdir())
    raw_valid_paths = sorted(raw_valid_path.iterdir())

    print("## Start preprocessing train datasets ##")
    preprocess('train', raw_train_paths)
    print("## End ##")
    print("## Start preprocessing valid datasets ##")
    preprocess('dev', raw_valid_paths)
    print("## End ##")


def preprocess(type, paths):
    count = 1
    split = 10_000
    file_num = 0
    file_out_path = Path(f'../data/pregenerated/{type}')
    file_out = (file_out_path/f'korquad2.1_{type}_{file_num}.jsonl').open('w', encoding='utf-8')

    for path in paths:
        file = json.load(path.open())
        for data in tqdm(file['data']):
            # split 수가 넘으면 파일 새로 생성
            if count // split > file_num:
                file_out.close()
                file_num += 1
                file_out = (file_out_path/f'korquad2.1_{type}_{file_num}.jsonl').open('w', encoding='utf-8')

            # 새로 넣을 dict 생성
            new_data = {}
            new_data.update({'title': data['title']})
            new_data.update({'context': replacing(data['context'])})

            # 같은 context에 대한 다른 question 분리
            for qas in data['qas']:
                new_data.update({'answer': {'text': qas['answer']['text'], 'answer_start': qas['answer']['answer_start']}})
                new_data.update({'question': qas['question']})
                new_data.update({'id': qas['id']})

                file_out.write(json.dumps(new_data, ensure_ascii=False)+'\n')
                count += 1

        print(f'Added dataset: {count}')

    file_out.close()
    print(f"data 수 : {count}")


def replacing(context):
    # context preprocess
    context = re.sub(r'<[%>]+\s+(?=<)|<[^>]+>', '', context).strip()
    context = re.sub(r'\n+', '\n', context).strip()
    context = re.sub(r'\t+', '\t', context).strip()
    return context


if __file__ == '__main__':
    owndataset()
