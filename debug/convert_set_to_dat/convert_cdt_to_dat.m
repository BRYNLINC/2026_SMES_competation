%% CDT文件批量转换为DAT格式
% 功能：将Neuroscan Curry的CDT格式转换为虚拟接收器可直接读取的DAT格式
% 特点：
%   1. 保存为.dat二进制文件，数据类型为float32
%   2. 按“每个时间点依次写出所有通道，最后一列为trigger”保存
%   3. 复制"顺序.txt"到输出目录，保留文件优先级信息
%   4. 为每个DAT文件额外生成同名_meta.txt，记录采样率、通道名、触发来源和存储格式

%% 配置路径
raw_data_dir = 'D:\dataset\DATA_BKW\fes';  % 原始CDT数据目录
output_dir = 'D:\dataset\BKW_converted_dat\fes';  % 转换后DAT文件保存目录

% 创建输出目录
if ~exist(output_dir, 'dir')
    mkdir(output_dir);
end

% 创建日志目录
log_dir = fullfile(output_dir, 'conversion_logs');
if ~exist(log_dir, 'dir')
    mkdir(log_dir);
end

%% 添加EEGLAB路径
% 如果使用EEGLAB，需要添加路径
% addpath('path/to/eeglab');
% eeglab nogui;

fprintf('========================================\n');
fprintf('CDT到DAT格式转换工具\n');
fprintf('========================================\n');
fprintf('原始数据目录: %s\n', raw_data_dir);
fprintf('输出目录: %s\n', output_dir);
fprintf('========================================\n\n');

%% 获取所有被试文件夹
subject_folders = dir(raw_data_dir);
subject_folders = subject_folders([subject_folders.isdir]);
subject_folders = subject_folders(~ismember({subject_folders.name}, {'.', '..'}));

fprintf('发现 %d 个被试文件夹\n\n', length(subject_folders));

%% 统计信息
total_files = 0;
success_files = 0;
failed_files = 0;
failed_list = {};

%% 处理每个被试
for i = 1:length(subject_folders)
    subject_name = subject_folders(i).name;
    subject_path = fullfile(raw_data_dir, subject_name);

    fprintf('========================================\n');
    fprintf('[%d/%d] 处理被试: %s\n', i, length(subject_folders), subject_name);
    fprintf('========================================\n');

    % 查找所有session文件夹
    session_folders = dir(fullfile(subject_path, 'session*'));
    session_folders = session_folders([session_folders.isdir]);

    if isempty(session_folders)
        fprintf('  [WARN] 未找到session文件夹，跳过\n\n');
        continue;
    end

    fprintf('  找到 %d 个session文件夹\n', length(session_folders));

    % 处理每个session
    for s = 1:length(session_folders)
        session_name = session_folders(s).name;
        session_path = fullfile(subject_path, session_name);

        fprintf('\n  --- 处理 %s ---\n', session_name);

        % 创建session输出目录
        session_output_dir = fullfile(output_dir, subject_name, session_name);
        if ~exist(session_output_dir, 'dir')
            mkdir(session_output_dir);
        end

        % 复制顺序.txt文件
        order_file = fullfile(session_path, '顺序.txt');
        if exist(order_file, 'file')
            copyfile(order_file, fullfile(session_output_dir, '顺序.txt'));
            fprintf('      [OK] 已复制顺序.txt\n');
        else
            fprintf('      [WARN] 未找到顺序.txt\n');
        end

        % 查找所有CDT文件
        cdt_files = dir(fullfile(session_path, '*.cdt'));

        % 排除.ceo和.dpa文件
        cdt_files = cdt_files(~contains({cdt_files.name}, '.ceo'));
        cdt_files = cdt_files(~contains({cdt_files.name}, '.dpa'));

        fprintf('      找到 %d 个CDT文件\n', length(cdt_files));

        % 处理每个CDT文件
        for j = 1:length(cdt_files)
            cdt_filename = cdt_files(j).name;
            cdt_filepath = fullfile(session_path, cdt_filename);

            total_files = total_files + 1;

            fprintf('      [%d/%d] 转换: %s\n', j, length(cdt_files), cdt_filename);

            try
                success = convert_single_cdt_to_dat(cdt_filepath, session_output_dir);

                if success
                    success_files = success_files + 1;
                    fprintf('          [OK] 转换成功\n');
                else
                    failed_files = failed_files + 1;
                    failed_list{end+1} = fullfile(subject_name, session_name, cdt_filename);
                    fprintf('          [FAIL] 转换失败\n');
                end

            catch ME
                failed_files = failed_files + 1;
                failed_list{end+1} = fullfile(subject_name, session_name, cdt_filename);
                fprintf('          [FAIL] 转换失败: %s\n', ME.message);
            end
        end
    end

    fprintf('\n');
end

%% 打印总结
fprintf('========================================\n');
fprintf('转换完成\n');
fprintf('========================================\n');
fprintf('总文件数: %d\n', total_files);
fprintf('成功: %d\n', success_files);
fprintf('失败: %d\n', failed_files);

if failed_files > 0
    fprintf('\n失败的文件列表:\n');
    for i = 1:length(failed_list)
        fprintf('  - %s\n', failed_list{i});
    end

    failed_log = fullfile(log_dir, sprintf('failed_conversions_%s.txt', datestr(now, 'yyyymmdd_HHMMSS')));
    fid = fopen(failed_log, 'w');
    for i = 1:length(failed_list)
        fprintf(fid, '%s\n', failed_list{i});
    end
    fclose(fid);
    fprintf('\n失败列表已保存到: %s\n', failed_log);
end

fprintf('\n提示：DAT文件已按“时间点 x 通道”的float32二进制格式保存，适用于VirtualReceiverImplement。\n');
fprintf('========================================\n');

%% 函数：转换单个CDT文件为DAT格式
function success = convert_single_cdt_to_dat(cdt_filepath, output_dir)
    success = false;

    % 检查文件是否存在
    if ~exist(cdt_filepath, 'file')
        fprintf('      [FAIL] 文件不存在\n');
        return;
    end

    % 检查文件大小
    file_info = dir(cdt_filepath);
    if file_info.bytes < 1024
        fprintf('      [FAIL] 文件太小，可能损坏\n');
        return;
    end

    try
        % 使用EEGLAB加载CDT文件
        EEG = pop_loadcurry(cdt_filepath);

        if isempty(EEG.data)
            fprintf('      [FAIL] 数据为空\n');
            return;
        end

        fprintf('      采样率: %.1f Hz\n', EEG.srate);
        fprintf('      通道数: %d\n', EEG.nbchan);
        fprintf('      时间点数: %d\n', EEG.pnts);
        fprintf('      试次数: %d\n', EEG.trials);
        fprintf('      时长: %.1f 秒\n', EEG.pnts / EEG.srate);
        fprintf('      事件数: %d\n', length(EEG.event));

        [~, filename, ~] = fileparts(cdt_filepath);
        output_filename = [filename '.dat'];
        output_filepath = fullfile(output_dir, output_filename);
        metadata_filepath = fullfile(output_dir, [filename '_meta.txt']);

        % 输出矩阵统一整理为 [时间点 x (EEG通道 + trigger)]，
        % 且保证 trigger 始终位于最后一列。
        [data_matrix, channel_labels, eeg_channel_count, trigger_source] = build_export_matrix(EEG);
        write_dat_matrix(output_filepath, data_matrix);

        % 记录元数据，便于后续读取和校验
        write_dat_metadata(metadata_filepath, EEG, output_filename, channel_labels, eeg_channel_count, trigger_source);

        fprintf('      已保存: %s\n', output_filename);
        fprintf('      已保存: %s\n', [filename '_meta.txt']);

        success = true;

    catch ME
        fprintf('      [FAIL] EEGLAB加载或写出失败: %s\n', ME.message);
    end
end

%% 函数：整理输出矩阵，确保trigger位于最后一列
function [data_matrix, output_channel_labels, eeg_channel_count, trigger_source] = build_export_matrix(EEG)
    raw_data = double(EEG.data);
    if ndims(raw_data) == 2
        raw_data = reshape(raw_data, size(raw_data, 1), size(raw_data, 2), 1);
    end

    input_channel_count = size(raw_data, 1);
    channel_labels = get_channel_labels(EEG, input_channel_count);
    trigger_channel_index = find_trigger_channel_index(channel_labels);

    if isempty(trigger_channel_index)
        eeg_data = raw_data;
        eeg_labels = channel_labels;
        trigger_matrix = build_trigger_from_events(EEG);
        trigger_source = 'event_reconstructed';
    else
        eeg_channel_indices = setdiff(1:input_channel_count, trigger_channel_index, 'stable');
        eeg_data = raw_data(eeg_channel_indices, :, :);
        eeg_labels = channel_labels(eeg_channel_indices);
        trigger_matrix = reshape(raw_data(trigger_channel_index, :, :), EEG.pnts, EEG.trials);
        trigger_source = sprintf('channel:%s', channel_labels{trigger_channel_index});
    end

    eeg_channel_count = size(eeg_data, 1);
    eeg_matrix = reshape(permute(eeg_data, [2 3 1]), [], eeg_channel_count);
    trigger_vector = reshape(trigger_matrix, [], 1);

    data_matrix = [eeg_matrix, trigger_vector];
    output_channel_labels = [eeg_labels, {'TRIGGER'}];
end

%% 函数：获取通道标签
function channel_labels = get_channel_labels(EEG, channel_count)
    if isfield(EEG, 'chanlocs') && ~isempty(EEG.chanlocs)
        channel_labels = {EEG.chanlocs.labels};
    else
        channel_labels = arrayfun(@(idx) sprintf('CH%d', idx), 1:channel_count, 'UniformOutput', false);
    end

    if length(channel_labels) < channel_count
        for idx = length(channel_labels) + 1:channel_count
            channel_labels{idx} = sprintf('CH%d', idx);
        end
    end
end

%% 函数：定位trigger通道
function trigger_channel_index = find_trigger_channel_index(channel_labels)
    trigger_channel_index = [];
    for idx = 1:length(channel_labels)
        normalized_label = upper(regexprep(string(channel_labels{idx}), '[^A-Z0-9]', ''));
        if contains(normalized_label, "TRIGGER") || contains(normalized_label, "TRIG") || ...
                contains(normalized_label, "STI014") || contains(normalized_label, "STATUS") || ...
                contains(normalized_label, "MARKER") || contains(normalized_label, "EVENT")
            trigger_channel_index = idx;
            return;
        end
    end
end

%% 函数：从事件列表重建trigger通道
function trigger_matrix = build_trigger_from_events(EEG)
    trigger_matrix = zeros(EEG.pnts, EEG.trials);
    if ~isfield(EEG, 'event') || isempty(EEG.event)
        return;
    end

    total_point_count = EEG.pnts * EEG.trials;
    for idx = 1:length(EEG.event)
        latency = double(EEG.event(idx).latency);
        if ~isfinite(latency)
            continue;
        end

        sample_index = round(latency);
        sample_index = max(1, min(total_point_count, sample_index));

        trial_index = ceil(sample_index / EEG.pnts);
        point_index = sample_index - (trial_index - 1) * EEG.pnts;
        point_index = max(1, min(EEG.pnts, point_index));
        trial_index = max(1, min(EEG.trials, trial_index));

        trigger_matrix(point_index, trial_index) = normalize_event_code(EEG.event(idx).type);
    end
end

%% 函数：将事件编码转为数值trigger
function event_code = normalize_event_code(event_type)
    event_code = 0;
    if isnumeric(event_type)
        event_code = double(event_type);
        return;
    end

    if iscell(event_type) && ~isempty(event_type)
        event_code = normalize_event_code(event_type{1});
        return;
    end

    event_text = char(string(event_type));
    numeric_code = str2double(event_text);
    if ~isnan(numeric_code)
        event_code = numeric_code;
        return;
    end

    match = regexp(event_text, '[-+]?\d+(\.\d+)?', 'match', 'once');
    if ~isempty(match)
        event_code = str2double(match);
    end
end

%% 函数：写出DAT元数据
function write_dat_metadata(metadata_filepath, EEG, output_filename, channel_labels, eeg_channel_count, trigger_source)
    fid = fopen(metadata_filepath, 'w');
    if fid == -1
        error('无法创建元数据文件: %s', metadata_filepath);
    end

    cleanup_obj = onCleanup(@() fclose(fid));

    fprintf(fid, 'data_file=%s\n', output_filename);
    fprintf(fid, 'data_layout=timepoints_by_channels\n');
    fprintf(fid, 'storage_format=binary_float32_le\n');
    fprintf(fid, 'value_order=sample_major_trigger_last\n');
    fprintf(fid, 'original_data_shape=channels_by_timepoints_by_trials\n');
    fprintf(fid, 'timepoints=%d\n', EEG.pnts);
    fprintf(fid, 'channels=%d\n', eeg_channel_count + 1);
    fprintf(fid, 'eeg_channels=%d\n', eeg_channel_count);
    fprintf(fid, 'trials=%d\n', EEG.trials);
    fprintf(fid, 'sampling_rate_hz=%.10g\n', EEG.srate);
    fprintf(fid, 'duration_seconds=%.10g\n', EEG.pnts / EEG.srate);
    fprintf(fid, 'event_count=%d\n', length(EEG.event));
    fprintf(fid, 'trigger_source=%s\n', trigger_source);
    fprintf(fid, 'channel_labels=%s\n', strjoin(channel_labels, ','));
end

%% 函数：写出DAT矩阵
function write_dat_matrix(output_filepath, data_matrix)
    fid = fopen(output_filepath, 'w', 'ieee-le');
    if fid == -1
        error('无法创建DAT文件: %s', output_filepath);
    end

    cleanup_obj = onCleanup(@() fclose(fid));
    fwrite(fid, single(data_matrix.'), 'float32');
end
