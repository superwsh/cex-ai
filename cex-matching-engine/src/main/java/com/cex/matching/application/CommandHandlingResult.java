package com.cex.matching.application;

/** 可靠撮合命令处理结果。 */
public enum CommandHandlingResult {
    /** 命令已完成持久化、内存执行和序列推进。 */
    APPLIED,
    /** 命令序列已被处理，本次未产生任何新副作用。 */
    DUPLICATE
}
