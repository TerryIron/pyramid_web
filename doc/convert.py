#!/usr/bin/env python
# coding=utf-8
#
#
# Copyright (c) 2015-2018  Terry Xi
# All Rights Reserved.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import os.path
import commands

input_dir = 'quick'
output_dir = 'src'

for i in commands.getoutput('find {0} -type f  | grep -v md$'.format(input_dir)).split('\n'):
    commands.getstatusoutput('cp {0} {1}'.format(i, output_dir))

for i in commands.getoutput('find {0} | grep md$'.format(input_dir)).split('\n'):
    t = os.path.basename(i)
    commands.getstatusoutput('pandoc --from=markdown --to=rst --output={0} {1}'.format(
        os.path.join(output_dir, '.'.join(t.split('.')[:-1])) + '.rst', i))
