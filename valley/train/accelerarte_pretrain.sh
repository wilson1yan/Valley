accelerate launch --main_process_port 6777 \
    --config_file ./configs/config.yaml valley/train/train_accelerate.py \
    --model_name_or_path ../weight_pretrained/vicuna-7b-v1.3 \
    --data_path ../meta_data/pretrain_data/LLaVA-CC3M-Pretrain-595K/chat.json \
    --video_data_path ../meta_data/pretrain_data/webvid_703K/chat.json \
    --image_folder ../meta_data/pretrain_data/LLaVA-CC3M-Pretrain-595K/image_new \
    --video_folder ../meta_data/finetune_data/Valley-Instruct/videos/webvid \
    --vision_tower openai/clip-vit-large-patch14 \
    --tune_mm_mlp_adapter True \
    --mm_vision_select_layer -2 \
    --tune_llm_layer none \
    --mm_use_im_start_end True \
    --bf16 False \
    --fp16 True \
    --output_dir ../checkpoints/zero3-pretrain-stable-valley-7b-v1 \
    --num_train_epochs 6 \
    --per_device_train_batch_size 16 \
    --per_device_eval_batch_size 1 \
    --gradient_accumulation_steps 1 \
    --evaluation_strategy no \
    --save_strategy steps \
    --save_steps 2400 \
    --save_total_limit 3 \
    --learning_rate 2e-3 \
    --weight_decay 0. \
    --warmup_ratio 0.03 \
    --lr_scheduler_type cosine \
    --logging_steps 1 \
    --tf32 False \
    --model_max_length 2048 \
    --gradient_checkpointing True \
    --lazy_preprocess True \
    --report_to wandb \
    --fast_epoch False 