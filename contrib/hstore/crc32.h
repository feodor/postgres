/*
 * contrib/hstore/crc32.h
 */
#ifndef _CRC32_H
#define _CRC32_H

/* Returns crc32 of data block */
extern unsigned int crc32_sz(char *buf, int size);

/* Returns crc32 of null-terminated string */
#define crc32(buf) crc32_sz((buf),strlen(buf))

/*
 * crc32 of multiple bufs
 */
extern unsigned int crc32_init(void);
extern unsigned int crc32_buf(unsigned int crc, char *buf, int size);
extern unsigned int crc32_fini(unsigned int crc);


#endif
